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

    tophat_read_mismatches=2,
    tophat_read_edit_dist=2,
    tophat_max_multihits=5,
    tophat_transcriptome_index="/projects/p20742/anno/tophat_tx/mm10.Ens_78.cuff",
    tophat_bowtie_index="/projects/p20742/anno/bowtie_indexes/mm10",
    quantification_transcriptome_index="{tophat_transcriptome_index}.gtf",

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
    mkdir -p "{basedir}/{run_name}/03_fastqc"
    mkdir -p "{basedir}/{run_name}/04_alignment"
    mkdir -p "{basedir}/{run_name}/05_quantification"
    mkdir -p "{basedir}/{run_name}/06_eda"
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
for index, data in enumerate(ssr.data[0:2]):
    if data["Sample_Project"] == arguments.values["project_id"]:
        tasks = []
        fastq_filenames = []
        for line in [1, 2, 3, 4]:
            sample_filename = "{}_S{}_L{:03}_R1_001".format(data["Sample_Name"], index+1, line)
            fastq_filenames.append("{basedir}/{run_name}/02_trimmed/{sample_filename}.trimmed.fastq.gz")
            current_t = pipeline.create_job(
                name="fastqc_{sample_filename}", 
                dependences=[t1],
                local_arguments=Arguments(
                    sample_id=data["Sample_ID"],
                    sample_name=data["Sample_Name"],
                    sample_filename=sample_filename))

            current_t.async_run("""
                module load fastqc/0.11.5
                module load java
                
                cp {basedir}/Data/Intensities/BaseCalls/{project_id}/{sample_id}/{sample_filename}.fastq.gz \
                    {basedir}/{run_name}/00_fastq
                fastqc -o {basedir}/{run_name}/01_fastqc {basedir}/{run_name}/00_fastq/{sample_filename}.fastq.gz
                java -jar /projects/b1038/tools/Trimmomatic-0.36/trimmomatic-0.36.jar SE \
                    -threads {num_processors} \
                    -phred33 {basedir}/{run_name}/00_fastq/{sample_filename}.fastq.gz \
                    {basedir}/{run_name}/02_trimmed/{sample_filename}.trimmed.fastq \
                    TRAILING:30 MINLEN:20 
                gzip {basedir}/{run_name}/02_trimmed/{sample_filename}.trimmed.fastq
                fastqc -o {basedir}/{run_name}/03_fastqc {basedir}/{run_name}/02_trimmed/{sample_filename}.trimmed.fastq.gz
                """)
            tasks.append(current_t)
        
        # Run tophat
        tophat_t = pipeline.create_job(
            name="tophat_{sample_name}",
            dependences=tasks,
            local_arguments=Arguments(
                sample_name=data["Sample_Name"],
                fastq_filenames=",".join(fastq_filenames)))

        tophat_t.async_run("""
            module load tophat/2.1.0
            module load samtools
            module load bowtie2/2.2.6
            module load boost
            module load gcc/4.8.3

        	tophat --no-novel-juncs \
                --read-mismatches {tophat_read_mismatches} \
                --read-edit-dist {tophat_read_edit_dist} \
                --num-threads {num_processors} \
                --max-multihits {tophat_max_multihits} \
                --transcriptome-index {tophat_transcriptome_index} \
                -o {basedir}/{run_name}/04_alignment/{sample_name} \
                {tophat_bowtie_index} \
                {fastq_filenames}
            ln -s {basedir}/{run_name}/04_alignment/{sample_name}/accepted_hits.bam {basedir}/{run_name}/04_alignment/{sample_name}.bam
            samtools index {basedir}/{run_name}/04_alignment/{sample_name}.bam
            htseq-count -f bam -q -m intersection-nonempty \
                -s reverse -t exon -i gene_id \
                {basedir}/{run_name}/04_alignment/{sample_name}.bam \
                {quantification_transcriptome_index} \
                > {basedir}/{run_name}/05_quantification/{sample_name}.htseq.counts
            """)
        step3_tasks.append(tophat_t)

t4 = pipeline.create_job(
    name="alignment_report",
    dependences=step3_tasks)
t4.async_run("""
    module load R/3.3.1
    Rscript /projects/p20742/tools/createTophatReport.R --topHatDir={basedir}/{run_name}/04_alignment/ --nClus={num_processors}
    """)


t5 = pipeline.create_job(
    name="quantification",
    dependences=[t4])
t5.async_run("""
    perl /projects/p20742/tools/makeHTseqCountsTable.pl {basedir}/{run_name}/04_alignment \
        {quantification_transcriptome_index} \
        {basedir}/{run_name}/05_quantification
    """)

pipeline.save_state(expanduser("~/pipeline_{}.json".format(timestamp)))
pipeline.close()
