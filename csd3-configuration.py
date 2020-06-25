import dataclasses
import importlib
import os
import sys

from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.utils import get_all_checkpoints
from typing import Callable

from workflowutils import wrap_shifter_container, wrap_singularity_container


@dataclasses.dataclass
class WorkflowConfig:
    ingest_source: str
    trim_ingest_list: int
    repo_dir: str
    root_softs: str
    wrap: Callable[[str], str]
    wrap_sql: Callable[[str], str]
    parsl_config: Config
    
def load_configuration():
    if len(sys.argv) < 2:
        raise RuntimeError("Specify configuration file as first argument")
    spec = importlib.util.spec_from_file_location('', sys.argv[1])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.configuration


# this is the directory that the workflow is invoked in and is where output
# files that don't go in the repo should be put.
workflow_cwd = os.getcwd()


# this is the directory that the workflow .py source code files live in
workflow_src_dir = os.path.dirname(os.path.abspath(__file__))

# initialize a Parsl worker environment (typically on batch node)
worker_init = """
cd {workflow_cwd}
#source setup.source
source /home/ir-perr3/parsl/setenv.sh
export PYTHONPATH={workflow_src_dir}  # to get at workflow modules on remote side
""".format(workflow_cwd=workflow_cwd, workflow_src_dir=workflow_src_dir)

worker_init_direct = """
cd {workflow_cwd}
source /home/ir-perr3/setenv.sh
export PYTHONPATH=$PYTHONPATH:{workflow_src_dir}  # to get at workflow modules on remote side
""".format(workflow_cwd=workflow_cwd, workflow_src_dir=workflow_src_dir)


csd3_queue_executor = HighThroughputExecutor(
    label='worker-nodes',
    address=address_by_hostname(),
    worker_debug=True,
    max_workers=10,               ## workers(user tasks)/node
    #cores_per_worker=30,          ## threads/user task

    # this overrides the default HighThroughputExecutor process workers
    # with process workers run inside the appropriate shifter container
    # with lsst setup commands executed. That means that everything
    # running in those workers will inherit the correct environment.

    heartbeat_period=600,
    heartbeat_threshold=1200,      ## time-out betweeen batch and local nodes
    provider=SlurmProvider(
        "skylake",                   ## queue/partition/qos
        nodes_per_block=1,        ## nodes per batch job
        exclusive=True,
        init_blocks=0,
        min_blocks=0,
        max_blocks=1,             ## max # of batch jobs
        parallelism=0,
        scheduler_options="""#SBATCH -A IRIS-IP005-CPU""",
        launcher=SrunLauncher(),
        cmd_timeout=300,          ## timeout (sec) for slurm commands
        walltime="04:00:00",
        worker_init=worker_init
    ),
)

csd3_queue_executor_direct = HighThroughputExecutor(
    label='worker-nodes',
    address=address_by_hostname(),
    worker_debug=True,
    max_workers=10,               ## workers(user tasks)/node
    #cores_per_worker=30,          ## threads/user task

    # this overrides the default HighThroughputExecutor process workers
    # with process workers run inside the appropriate shifter container
    # with lsst setup commands executed. That means that everything
    # running in those workers will inherit the correct environment.

    heartbeat_period=600,
    heartbeat_threshold=1200,      ## time-out betweeen batch and local nodes
    provider=SlurmProvider(
        "skylake",                   ## queue/partition/qos
        nodes_per_block=1,        ## nodes per batch job
        exclusive=True,
        init_blocks=0,
        min_blocks=0,
        max_blocks=1,             ## max # of batch jobs
        parallelism=0,
        scheduler_options="""#SBATCH -A IRIS-IP005-CPU""",
        launcher=SrunLauncher(),
        cmd_timeout=300,          ## timeout (sec) for slurm commands
        walltime="04:00:00",
        worker_init=worker_init_direct
    ),
)

local_executor = ThreadPoolExecutor(max_threads=2, label="submit-node")

csd3_singularity_debug_config = WorkflowConfig(
    ingest_source="/home/ir-perr3/rds/rds-iris-ip005/jamesp/parsl/input",
    trim_ingest_list = None,

  # this is the butler repo to use
#  repo_dir="/global/cscratch1/sd/bxc/parslTest/test0",
#    repo_dir = "/global/cscratch1/sd/descdm/tomTest/DRPtest1",
    repo_dir = "/home/ir-perr3/rds/rds-iris-ip005/jamesp/parsl/repo",

#  root_softs="/global/homes/b/bxc/dm/",
    root_softs="/home/ir-perr3/parsl",
  # what is ROOT_SOFTS in general? this has come from the SRS workflow,
  # probably the path to this workflow's repo, up one level.


  # This specifies a function (str -> str) which rewrites a bash command into
  # one appropriately wrapped for whichever container/environment is being used
  # with this configuration (for example, wrap_shifter_container writes the
  # command to a temporary file and then invokes that file inside shifter)
    wrap=wrap_singularity_container,
    wrap_sql=wrap_singularity_container,

    parsl_config=Config(
        executors=[local_executor, csd3_queue_executor],
        app_cache=True,
        checkpoint_mode='task_exit',
        checkpoint_files=get_all_checkpoints(),
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            monitoring_debug=False,
            resource_monitoring_interval=10
        )
    )
)

def wrap_no_op(s):
    return s

csd3_direct_debug_config = WorkflowConfig(
    ingest_source="/home/ir-perr3/rds/rds-iris-ip005/jamesp/parsl/input",
    trim_ingest_list = None,

  # this is the butler repo to use
#  repo_dir="/global/cscratch1/sd/bxc/parslTest/test0",
#    repo_dir = "/global/cscratch1/sd/descdm/tomTest/DRPtest1",
    repo_dir = "/home/ir-perr3/rds/rds-iris-ip005/jamesp/parsl/repo",

#  root_softs="/global/homes/b/bxc/dm/",
    root_softs="/home/ir-perr3/parsl",
  # what is ROOT_SOFTS in general? this has come from the SRS workflow,
  # probably the path to this workflow's repo, up one level.


  # This specifies a function (str -> str) which rewrites a bash command into
  # one appropriately wrapped for whichever container/environment is being used
  # with this configuration (for example, wrap_shifter_container writes the
  # command to a temporary file and then invokes that file inside shifter)
    wrap=wrap_no_op,
    wrap_sql=wrap_no_op,

    parsl_config=Config(
        executors=[local_executor, csd3_queue_executor_direct],
        app_cache=True,
        checkpoint_mode='task_exit',
        checkpoint_files=get_all_checkpoints(),
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            monitoring_debug=False,
            resource_monitoring_interval=10
        )
    )
)

configuration = csd3_singularity_debug_config
#configuration = csd3_direct_debug_config


