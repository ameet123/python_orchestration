import logging
import subprocess
from subprocess import CalledProcessError
import time
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger('AnthemOrchestrator')
CONFIG_FILE = 'C:/Users/ameet.chaubal/Documents/source/anthemOrchestrator/workflow.dat'
COMMAND_COL = 'command'
PROJECT_COL = 'project name'
STAGE_COL = 'stage'
PARALLEL_COL = 'parallel'
UTF = "utf-8"

data = pd.read_csv(CONFIG_FILE, header=0)


def cleanseCommand(cmd):
    args = cmd.strip().split()
    LOGGER.debug("cmd args:%s", args)
    return args

start=time.time()
LOGGER.info("Start:->")
for index, row in data.iterrows():
    cmd = row[COMMAND_COL]
    project = row[PROJECT_COL]
    stage = row[STAGE_COL]
    cmdArray = cleanseCommand(cmd)
    LOGGER.info("Proj[%s]: stage:[%s] Cmdarray:%s", project, stage, cmdArray)
    try:
        proc = subprocess.Popen(cmdArray, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        output, error = proc.communicate()
        error = error.decode(UTF)
        output = output.decode(UTF)
        proc.wait()
        LOGGER.debug("\tRETURNCODE:%s\n\t output:%s\n\t error:%s", proc.returncode, output, error)

    except OSError as o:
        print("OSERROR:{}".format(o))
    except CalledProcessError as e:
        out = e.output
        print("output:{}".format(out))
        print("code:{} err:{}".format(e.returncode, e.stderr))
        # print("out---->{}".format(status))
    except ValueError as v:
        print("VALUE:{}".format(v))

elapsed=time.time()-start
LOGGER.info("<-- Completed in:%.2f sec.", elapsed)