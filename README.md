### Orchestration

This script enables multiple shell scripts to be chained together in a desired sequence.
The scripts can be executed in parallel or in sequence.

#### Execution

```commandline
Orchestrator.py workflow.dat
```

#### Workflow File

This file describes all the scripts to be executed in appropriate sequence.
A sample is as follows,

```text
project name,stage,command,isParallel
myProject,1,/bin/sh /home/af55267/script/orchestrate/python_orchestration/sleeper.sh,yes
team-2,2,ls,yes
```

