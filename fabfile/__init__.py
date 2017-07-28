from fabric.api import cd, env, task, run, settings
from questmon import Pipeline

import json


env.hosts = []

@task
def load_quest(src):
    env.hosts = ["quest.it.northwestern.edu"]
    env.environment = "quest"
    env.questmon_folder = src

@task
def git_pull():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("git pull origin master")
    
@task
def run_test_pipeline():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/ActivePython-3.2 ; python3.2 run_test_pipeline.py")

@task
def checkjobs():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/ActivePython-3.2 ; python3.2 checkjobs.py")
