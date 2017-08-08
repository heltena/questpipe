from datetime import datetime 
from os.path import expanduser
from pathlib import Path

DEPS_A = "[quest.northwestern.edu] out: dep: "
DEPS_B = "[quest.northwestern.edu] out: I: dep: "
RUNNING = "[quest.northwestern.edu] out: I: Running "

PROLOGUE = "PBS: Begin PBS Prologue"
EPILOGUE = "Begin PBS Epilogue"
EXIT_VALUE = "Job exit value:"

tests = [
    ("pipeline_20170808_094817.log", "result_20170808_094817.csv", expanduser("~/current_project/Result_20170808_094817/logs/")),  # This is bad (-w)
    ("pipeline_20170808_113245.log", "result_20170808_113245.csv", expanduser("~/current_project/Result_20170808_113245/logs/")),  # This is good (-l)
    ("pipeline_20170808_162553.log", "result_20170808_162553.csv", expanduser("~/current_project/Result_20170808_162553/logs/"))   # This is bad, again (-w)
    ]

pipeline_filename, result_filename, result_path = tests[2]

job_dependency = {}
with open(pipeline_filename, "rt") as f:
    deps = None
    for i, line in enumerate(f.readlines()):
        line = line.strip()
        if line.startswith(DEPS_A):
            deps = eval(line[len(DEPS_A):])
            if type(deps) == str:
                deps = [deps]
        if line.startswith(DEPS_B):
            deps = eval(line[len(DEPS_B):])
            if type(deps) == str:
                deps = [deps]
        if line.startswith(RUNNING):
            job_id = line[len(RUNNING):]
            job_dependency[job_id] = deps
            deps = None

job_values = {}
p = Path(result_path)
for x in p.glob("*"):
    n = x.name
    s = x.suffix
    if not s.startswith(".o"):
        break
    job_id = s[2:]

    sbegin_time = None
    send_time = None
    exit_value = None
    with open(x, "rt") as f:
        for i, line in enumerate(f.readlines()):
            line = line.strip()
            if line.startswith(PROLOGUE):
                sbegin_time = line[len(PROLOGUE):]
            elif line.startswith(EPILOGUE):
                send_time = line[len(EPILOGUE):]
            elif line.startswith(EXIT_VALUE):
                exit_value = line[len(EXIT_VALUE):]

    begin_time = datetime.strptime(sbegin_time[0:sbegin_time.rfind(' ')].strip(), "%a %b %d %H:%M:%S %Z %Y")
    end_time = datetime.strptime(send_time[0:send_time.rfind(' ')].strip(), "%a %b %d %H:%M:%S %Z %Y")
    job_values[job_id] = (begin_time, end_time, sbegin_time, send_time, exit_value)

with open(result_filename, "wt") as f:
    f.write("job_id,begin_time,end_time,exit_value,dependencies\n")
    for job_id in sorted(job_dependency.keys()):
        dep = job_dependency[job_id]
        if dep is None:
            dep = []
        dependencies = ":".join(dep)
        begin_time, end_time, sbegin_time, send_time, exit_value = job_values.get(job_id, (None, None, None, None, None))
        
        for cur_dep in dep:
            other_begin, other_end, _, _, _ = job_values[cur_dep]
            if other_end > begin_time:
                print("ERROR! {} < {} ({}, {})".format(begin_time, other_end, job_id, cur_dep))
        f.write("{},{},{},{},{}\n".format(job_id, sbegin_time, send_time, exit_value, dependencies))