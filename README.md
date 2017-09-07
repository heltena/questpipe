# QUESTPIPE

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

* load_quest: loads the data to connect to the server. The parameter is the source of the 'questpipe' folder.
* sync: copies (rsync) the files changed on the loca folder to the questpipe folder:

    `
    fab -u <username> load_quest:~/src/questpipe sync
    `

* run_pipeline_pool: runs pulrseq pipeline example.

    `
    fab -u <username> load_quest:~/src/questpipe run_pipeline_pool
    `

* checkjobs: checks if the jobs of the pipeline are completed.

    `
    fab -u <username> load_quest:~/src/questpipe checkjobs:file_of_the_pipeline
    `

* abort_pipeline: aborts all the jobs of the pipeline:

    `
    fab -u <username> load_quest:~/src/questpipe abort_pipeline:file_of_the_pipeline
    `

Note: you can run two or more tasks in a single call:


    fab -u <username> load_quest:~/src/questpipe sync run_pipeline_pool


## Connection to Quest

Fabric uses SSH Key to connect to quest. To do this, create or use a SSH key pair of your computer and copy the public key to your ~/.ssh/authorized_keys (see manual).

## Examples of pipelines

In order to run these examples, the Python code should be run on Quest. For example:

    $ ssh user@quest.northwestern.edu
    quest> $ module load python/anaconda3.6
    quest> $ python example1.py

As an alternative, Fabric can be used creating a task that runs the code below. In the fabric file:

    @task
    def our_task():
        with settings(user=env.user), cd(env.questpipe_folder):
            run("module load python/anaconda3.6 ; python3.6 our_task_file.py")

And then, using a console in our local terminal:

    fab -u user load_quest:~/src/questpipe sync our_task

Run the task "sync" before running the "our_task" task is important. Otherwise, the previous code will be run instead of the current one.

# Example 1: just a pipeline

In this example, a pipeline is created using different parameters. It is not creating jobs:

    import questpipe as qp
    from os.path import expanduser
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = "Result_{}".format(timestamp)

    arguments = qp.Arguments(
        run_name=run_name,

        basedir=expanduser("~"),
        project_name="example_test",
        project_dir="{basedir}/{project_name}",
        rundir="{project_dir}/{run_name}"
    )

    with qp.Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments) as pipeline:
        pipeline.debug_to_filename("{rundir}/pipeline.log", create_parent_folders=True)
        pipeline.save_state("{rundir}/pipeline.json")
