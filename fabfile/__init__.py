from fabric.api import cd, env, task, local, run, settings
from fabric.contrib.project import rsync_project
import questpipe as qp

import json


env.hosts = []

@task
def load_quest(src):
    env.hosts = ["quest.northwestern.edu"]
    env.environment = "quest"
    env.questpipe_folder = src

@task
def sync():
    with settings(user=env.user):
        rsync_project(local_dir=".", remote_dir=env.questpipe_folder, exclude=[".git", "ssh_keys"])
    
@task
def run_test_pipeline():
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 run_test_pipeline.py")

@task
def run_pipeline_pool(from_index, to_index):
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 run_pipeline_pool.py {} {}".format(from_index, to_index))

@task
def checkjobs(pipeline_name):
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 checkjobs.py {}".format(pipeline_name))

@task
def abort_pipeline(pipeline_name):
    with settings(user=env.user), cd(env.questpipe_folder):
        run("module load python/anaconda3.6 ; python3.6 abort_pipeline.py {}".format(pipeline_name))