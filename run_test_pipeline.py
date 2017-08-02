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

# STEP 1: Create the folders to store the data
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

# STEP 2: Create fastq files
t1 = pipeline.create_job(name="bcl2fastq")
t1.async_run("""
    module load bcl2fastq/2.17.1.14
    echo bcl2fastq -R {basedir} -r {num_processors} -d {num_processors} -p {num_processors} -w {num_processors}
    """)

# STEP 3: Create the fastqc files from fastq
step3_tasks = []
sample_sheet_filename = pipeline.parse_string("{basedir}/SampleSheet.csv")
ssr = SampleSheetLoader(sample_sheet_filename)
for index, data in enumerate(ssr.data)[0:2]:
    if data["Sample_Project"] == arguments.values["project_id"]:
        for line in [1, 2, 3, 4]:
            filename = "{}_S{}_L{:04}_R1_001".format(data["Sample_Name"], index+1, line)

            copy_command = pipeline.parse_string("cp {}/{}/{}.fastq.gz {}".format(
                "{basedir}/Data/Intensities/BaseCalls/{project_id}", 
                data["Sample_ID"],
                filename,
                "{basedir}/{run_name}/00_fastq"))

            current_t = pipeline.create_job(
                name="fastqc_{}".format(sample_filename), 
                dependences=[t1],
                local_arguments=Arguments(
                    copy_command=copy_command,
                    sample_filename=sample_filename))

            current_t.async_run("""
                module load fastqc/0.11.5
                module load java
                
                {copy_command}
                fastqc {basedir}/{run_name}/00_fastq/{sample_filename}.fastq.gz -o {basedir}/{run_name}/01_fastqc
                java -jar /projects/b1038/tools/Trimmomatic-0.36/trimmomatic-0.36.jar SE -threads {num_processors} -phred33 {basedir}/{run_name}/01_fastqc/{sample_filename}.fastq.gz {basedir}/{run_name}/02_trimmed/{sample_filename}.fastq TRAILING:30 MINLEN:20 
                gzip {basedir}/{run_name}/02_trimmed/{sample_filename}.fastq
                """)
            step3_tasks.append(current_t)


pipeline.save_state(expanduser("~/pipeline_{}.json".format(timestamp)))
pipeline.close()
