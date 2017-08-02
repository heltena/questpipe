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

    basedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_questmon/160728_NB501488_0018_AHFJJVBGXY",
    project_name="160728_NB501488_0018_AHFJJVBGXY",
    project_id="160728_AM",

    workdir="{basedir}/",
    outdir="{basedir}/{run_name}/logs/",
    errdir="{basedir}/{run_name}/logs/",
)

pipeline = Pipeline(name="mypipeline", join_command_arguments=True, arguments=arguments)
_, stdout, stderr = pipeline.run("""
    mkdir -p "{basedir}/{run_name}"
    mkdir -p "{basedir}/{run_name}/logs"
    mkdir -p "{basedir}/{run_name}/00_fastq"
    mkdir -p "{basedir}/{run_name}/01_fastqc"
    mkdir -p "{basedir}/{run_name}/02_trimmed"
    mkdir -p "{basedir}/{run_name}/03_alignment"
    mkdir -p "{basedir}/{run_name}/04_quantification"
    mkdir -p "{basedir}/{run_name}/05_eda"
""")

t1 = pipeline.create_job(name="bcl2fastq")
t1.async_run("""
    module load bcl2fastq/2.17.1.14
    echo bcl2fastq -R {basedir} -r {num_processors} -d {num_processors} -p {num_processors} -w {num_processors}
    """)

sample_sheet_filename = pipeline.parse_string("{basedir}/SampleSheet.csv")

sample_filenames = []

ssr = SampleSheetLoader(sample_sheet_filename)
for index, data in enumerate(ssr.data):
    if data["Sample_Project"] == arguments.values["project_id"]:
        for line in [1, 2, 3, 4]:
            filename = "{}_S{}_L{:04}_R1_001.fastq.gz".format(data["Sample_Name"], index+1, line)
            sample_filenames.append(filename)

commands = []
for sample_filename in sample_filenames:
    command = "echo cp {}/{} {}".format(
        "{basedir}/Data/Intensities/BaseCalls/{project_id}", 
        sample_filename,
        "{basedir}/{run_name}/00_fastq")
    commands.append(files_to_copy)

t2 = pipeline.create_job(name="copy_fastq", dependences=[t1])
t2.async_run("\n".join(commands))

pipeline.save_state(expanduser("~/pipeline.json"))
pipeline.close()
