# QUESTMON

Submit jobs to Quest MOAB scheduler integrating it in your awesome python code.


## Install

* clone the code on your computer.
* create a virtualenv using Python 3.2 or newer.
* install the packages listed on requirements.txt

## Run Fabric

Fabric accepts this global arguments:

* -u &lt;username&gt;
* -H &lt;host&gt;

Fabric has this tasks:

* load_quest: load the data to connect to the server. The parameter is the source of the 'questmon' folder.
* git_pull: pull the code on the host you listed:

    `
    fab -u <username> load_quest:~/src/questmon git_pull
    `

* run_test_pipeline: run two jobs in parallel and another one when these finish.

    `
    fab -u <username> load_quest:~/src/questmon run_test_pipeline
    `

* checkjobs: check if the jobs of the previous pipeline are completed.

    `
    fab -u <username> load_quest:~/src/questmon checkjobs
    `

Note: you can run two or more tasks in a single call:

    `
    fab -u <username> load_quest:~/src/questmon git_pull run_test_pipeline
    `

## Connection to Quest

Fabric uses SSH Key to connect to quest. To do this, create or use a SSH key pair of your computer and copy the public key to your ~/.ssh/authorized_keys (see manual).
