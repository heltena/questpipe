from fabric.api import cd, env, task, local, run, settings
from questmon import Pipeline

import json


env.hosts = []

@task
def load_quest(src):
    env.hosts = ["quest.it.northwestern.edu"]
    env.environment = "quest"
    env.questmon_folder = src

@task
def commit(message):
    local("git commit -a -m \"{}\"".format(message))
    local("git push")

@task
def git_pull():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("git pull origin heltena")
    
@task
def run_kishore_pipeline():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/anaconda3.6 ; python3.6 run_kishore_pipeline.py")

@task
def run_test_pipeline():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/anaconda3.6 ; python3.6 run_test_pipeline.py")

@task
def checkjobs():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/anaconda3.6 ; python3.6 checkjobs.py")

@task
def abort_pipeline():
    with settings(user=env.user):
        with cd(env.questmon_folder):
            run("module load python/anaconda3.6 ; python3.6 abort_pipeline.py")

@task
def test():
    from questmon import Arguments
    gl = Arguments(pepe=1, juan=2)
    loc = Arguments(andres=2, juan=3)
    print(gl.combine(loc))
    print(loc.combine(gl))
    print(gl)
    print(loc)