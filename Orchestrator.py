#!/usr/bin/env python

import logging
import os
import subprocess
import sys
import time
from subprocess import CalledProcessError

import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger('AnthemOrchestrator')

COMMAND_COL = 'command'
PROJECT_COL = 'project name'
STAGE_COL = 'stage'
PARALLEL_COL = 'isParallel'
UTF = "utf-8"
DATA = None
WORKFLOW_ERROR_CODE = 5
STAGES = None
OUTPUT_MAX_LEN = 1024
STATUS_UNDERLINE_LEN = 120


# To orchestrate multiple stages in defined sequence.
# Assumption: a text file absolute path will be provided
# this file contains 3 columns: project name,stage,command
class Orchestrate:
    workFlow = None
    START = None

    def __init__(self, workflow):
        self.workFlow = workflow

    def cleanseCommand(self, cmd):
        args = cmd.strip().split()
        LOGGER.debug("cmd args:%s", args)
        return args

    def init(self):
        global START, DATA, STAGES
        START = time.time()

        try:
            DATA = pd.read_csv(self.workFlow, header=0)
        except:
            LOGGER.error("Error reading workflow file:%s", sys.exc_info()[1])
            sys.exit(WORKFLOW_ERROR_CODE)
        STAGES = DATA.shape[0]
        LOGGER.info("Start:-> Stages:%d", STAGES)

    def finish(self):
        global START
        elapsed = time.time() - START
        LOGGER.info("<-- Completed in:%.2f sec.", elapsed)

    def stageLaunch(self, project, stage):
        status = "\t\t------ [%s] %s:%s --------"
        status = status + "\n" + STATUS_UNDERLINE_LEN * "="
        LOGGER.info(status, "START", project, stage)
        return time.time()

    def stageEnd(self, project, stage, start):
        status = "\t\t------ [%s] %s:%s (%.2f sec)--------"
        status = status + "\n" + STATUS_UNDERLINE_LEN * "="
        LOGGER.info(status, "END", project, stage, time.time() - start)

    def process(self):
        procs = []
        for index, row in DATA.iterrows():
            cmd = row[COMMAND_COL]
            project = row[PROJECT_COL]
            stage = row[STAGE_COL]
            cmdArray = self.cleanseCommand(cmd)
            isParallel = row[PARALLEL_COL]
            LOGGER.info("Proj[%s]: stage:[%s] Cmdarray:%s", project, stage, cmdArray)
            try:
                start = self.stageLaunch(project, stage)
                proc = subprocess.Popen(cmdArray, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                ps = ProcessStruct(project, stage, proc, cmd, start)
                if isParallel == "yes":
                    LOGGER.debug("Process:%s stage:%s is parallel, moving to next", project, stage)
                    procs.append(ps)
                    continue
                else:
                    LOGGER.debug("Process:%s stage:%s is sequential, waiting for completion", project, stage)
                    self.processStatus(ps)
                    self.stageEnd(ps.name, ps.stage, ps.start)

            except OSError as o:
                LOGGER.error("Err: oserror:%s", o.strerror)
            except CalledProcessError as e:
                LOGGER.debug("return:%s err:%s output:%s", e.returncode, e.stderr, e.output)
            except ValueError as v:
                LOGGER.error("Err: value:%s", v)

        LOGGER.debug("Launch completed, will wait on completion: %d stages", len(procs))
        for p in procs:
            LOGGER.info("waiting on proc:%s", p.name)
            self.processStatus(p)
            # proc = p.proc
            # output, error = proc.communicate()
            # error = error.decode(UTF)
            # output = output.decode(UTF)
            # LOGGER.info("\t%s {stage:%s, return:%s, output:\"%s\", error:\"%s\"}",
            #             p.name, p.stage, proc.returncode, output[:OUTPUT_MAX_LEN], error)
            self.stageEnd(p.name, p.stage, p.start)

    def processStatus(self, p):
        LOGGER.info("waiting on proc:%s", p.name)
        proc = p.proc
        output, error = proc.communicate()
        error = error.decode(UTF)
        output = output.decode(UTF)
        LOGGER.info("\t%s {stage:%s, return:%s, output:\"%s\", error:\"%s\"}",
                    p.name, p.stage, proc.returncode, output[:OUTPUT_MAX_LEN], error)
        self.stageEnd(p.name, p.stage, p.start)


class ProcessStruct:
    name = None
    stage = None
    proc = None
    cmd = None
    start = None

    def __init__(self, name, stage, proc, cmd, start):
        self.start = start
        self.name = name
        self.stage = stage
        self.proc = proc
        self.cmd = cmd


if __name__ == '__main__':
    if len(sys.argv) < 2:
        LOGGER.error("Usage: %s <name of workflow file>", os.path.basename(sys.argv[0]))
        sys.exit(1)

    fileName = sys.argv[1]
    LOGGER.info("WorkFlow:%s", fileName)
    orch = Orchestrate(fileName)
    orch.init()
    orch.process()
    orch.finish()
