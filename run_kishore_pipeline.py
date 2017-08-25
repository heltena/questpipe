from questmon import Arguments, Pipeline
from os.path import expanduser


arguments = Arguments(
    msub_arguments=[
        "-A b1042",
        "-q pulrseq",
        "-l walltime=24:00:00,nodes=1:ppn=8",
        "-m a",
        "-j oe",
        "-W umask=0113",
        "-N {job_name}"],
    workdir=expanduser("~"),
    outdir=expanduser("~"),
    errdir=expanduser("~")
)

pipeline = Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments)
pipeline.debug_to_filename("{outdir}/pipeline.log", create_parent_folders=True)

t1 = pipeline.create_job(name="first_task")
t1.prepare_async_run("""
    sleep 5
    echo {job_name} >> sophia001
    """)

t2 = pipeline.create_job(name="second_task")
t2.async_run("""
    sleep 10
    echo {job_name} >> helio001
    """)

t3 = pipeline.create_job(name="third_task", dependences=[t1, t2])
t3.async_run("""
    sleep 1
    echo {job_name} >> FINISHED
    """)

t1.unhold()

pipeline.save_state("{outdir}/pipeline.json")
