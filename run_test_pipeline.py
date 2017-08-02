from questmon import Arguments, Pipeline
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

    basedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_questmon/160728_NB501488_0018_AHFJJVBGXY",
    project_name="160728_NB501488_0018_AHFJJVBGXY",
    projectdir="{basedir}",

    workdir="{projectdir}/",
    outdir="{projectdir}/{run_name}",
    errdir="{projectdir}/{run_name}",
)

pipeline = Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments)

t1 = pipeline.create_job(name="blc2fastq")
t1.async_run("""
    module load bcl2fastq/2.17.1.14
    echo bcl2fastq -R {basedir}/{project_name} -r {num_processors} -d {num_processors} -p {num_processors} -w {num_processors}
    """)

pipeline.save_state(expanduser("~/pipeline.json"))
pipeline.close()
