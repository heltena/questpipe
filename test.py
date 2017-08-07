from questmon import Arguments, Pipeline, SampleSheetLoader
from os.path import expanduser
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
run_name = "Result_{}".format(timestamp)

arguments = Arguments(
    num_processors=10,
    run_name=run_name,

    msub_arguments=[
        "-A b1042",
        "-q pulrseq",
        "-l walltime=24:00:00,nodes=1:ppn={num_processors}",
        "-m a",
        "-j oe",
        "-W umask=0113",
        "-N {job_name}"],

    basedir=expanduser("~"),
    project_name="example_test",
    project_dir="{basedir}/{project_name}",
    rundir="{project_dir}/{run_name}",

    workdir="{project_dir}/",
    outdir="{rundir}/logs",
    errdir="{rundir}/logs",
)

pipeline = Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments, debug=True)
_, stdout, stderr = pipeline.run("""
    mkdir -p "{rundir}"
    mkdir -p "{rundir}/logs"
    echo "hola"
    """)

t1 = pipeline.create_job(name="test")
t1.prepare_async_run("""
    echo "Start t1"
    sleep 1
    echo "Finished"
    """)

t2 = pipeline.create_job(name="lock", dependences=[t1])
t2.async_run("""
    echo "t2 is started"
    sleep 0
    echo "t2 finished"
    """)

tasks = []
for i in range(3):
    dependences = [t2] + tasks[-5:]
    current_t = pipeline.create_job(name="test_{}".format(i), dependences=dependences)
    current_t.async_run("""
        echo "t_{job_name} is started"
        sleep 10
        echo "t_{job_name} finished"
        """)
    tasks.append(current_t)

t3 = pipeline.create_job(name="final_task", dependences=tasks)
t3.async_run("""
    echo "t_{job_name} is started"
    sleep 10
    echo "t_{job_name} finished"
    """)

t1.unhold()

pipeline.save_state(expanduser("~/pipeline_{}.json".format(timestamp)))
pipeline.close()
