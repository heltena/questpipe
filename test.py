import questpipe as qp
import questpipe.illumina as qpi
from os.path import expanduser
from datetime import datetime


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
run_name = "Result_{}".format(timestamp)

arguments = qp.Arguments(
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

with qp.Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments) as pipeline:
    pipeline.debug_to_filename("{rundir}/pipeline.log", create_parent_folders=True)

    t1 = pipeline.create_job(name="helio")
    t1.prepare_async_run("""
        echo "HOLA" >{rundir}/helio.txt
        """)

    t2 = pipeline.create_job(name="sophia", dependences=[t1])
    t2.async_run("""
        sleep 10
        echo "ADIOS" >>{rundir}/helio.txt
        """)

    t1.unhold()
    
    pipeline.save_state("{rundir}/pipeline.json")
