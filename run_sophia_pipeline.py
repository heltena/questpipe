from datetime import datetime
import questpipe as qp
import questpipe.illumina as qpi
import questpipe.helpers as qph
import time


project_name, project_id = ('160728_NB501488_0018_AHFJJVBGXY', '160728_AM')           # DONE


print("Running project {}...".format(project_name))

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
run_name = "Result_20170913_170957"

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

    basedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_trimming",
    project_id=project_id,
    project_name=project_name,
    project_dir="{basedir}/{project_name}",

    rundir="{project_dir}/{run_name}",
    filedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_questmon2/{project_name}",
    fileloc="{filedir}/latest",
    illumina_csv_sheet="{filedir}/SampleSheet.csv",

    tophat_read_mismatches=2,
    tophat_read_edit_dist=2,
    tophat_max_multihits=20,
    tophat_transcriptome_index="/projects/p20742/anno/tophat_tx/mm10.Ens_78.cuff",  # tophat uses GFF files and it does not like the extension :-(
    tophat_bowtie_index="/projects/p20742/anno/bowtie_indexes/mm10",
    quantification_transcriptome_index="/projects/p20742/anno/Ens/mm10.Ens_78/mm10.Ens_78.cuff.gtf",

    workdir="{project_dir}/",
    outdir="{rundir}/logs/",
    errdir="{rundir}/logs/",
)

with qp.Pipeline(name=run_name, join_command_arguments=True, arguments=arguments) as pipeline:
    pipeline.debug_to_filename("{rundir}/pipeline.log", create_parent_folders=True)

    # STEP 1: Create the folders to store the data
    # _, stdout, stderr = pipeline.run("""
    #     mkdir -p "{rundir}"
    #     mkdir -p "{rundir}/logs"
    #     mkdir -p "{rundir}/00_fastq"
    #     mkdir -p "{rundir}/01_fastqc"
    #     mkdir -p "{rundir}/02_trimmed"
    #     mkdir -p "{rundir}/03_fastqc"
    #     mkdir -p "{rundir}/03.5_multifastqc"
    #     mkdir -p "{rundir}/04_alignment"
    #     mkdir -p "{rundir}/05_quantification"
    #     mkdir -p "{rundir}/06_eda"
    # """)

    # STEP 2: Create fastq files
    # t1 = pipeline.create_job(name="00_bcl2fastq")
    # t1.prepare_async_run("""
    #     module load bcl2fastq/2.17.1.14
    #     bcl2fastq -R {project_dir} -r {num_processors} -d {num_processors} -p {num_processors} -w {num_processors}
    #     """)

    # STEP 3: Create the fastqc files from fastq
    step3_tasks = []
    sample_sheet_filename = pipeline.parse_string("{illumina_csv_sheet}")
    ssr = qpi.SampleSheetLoader(sample_sheet_filename)
    for index, data in enumerate(ssr.data):
        if data["Sample_Project"] != arguments.values["project_id"]:
            continue

        # tasks = []
        fastq_filenames = []
        # for line in [1, 2, 3, 4]:
        #     sample_filename = "{}_S{}_L{:03}_R1_001".format(data["Sample_Name"], index+1, line)
        #     fastq_filenames.append("{rundir}/02_trimmed/{sample_filename}.trimmed.fastq.gz")

        #     current_t = pipeline.create_job(
        #         name="01_fastqc_{sample_filename}",
        #         local_arguments=qp.Arguments(
        #             sample_id=data["Sample_ID"],
        #             sample_name=data["Sample_Name"],
        #             sample_filename=sample_filename))

        #     current_t.async_run("""
        #         module load fastqc/0.11.5
        #         module load java
        #         """)
        #         # java -jar /projects/b1038/tools/Trimmomatic-0.36/trimmomatic-0.36.jar SE \
        #         #     -threads {num_processors} \
        #         #     -phred33 {fileloc}/00_fastq/{sample_filename}.fastq.gz \
        #         #     {rundir}/02_trimmed/{sample_filename}.trimmed.fastq \
        #         #     TRAILING:30 CROP:72 HEADCROP:15 MINLEN:36
        #         # gzip {rundir}/02_trimmed/{sample_filename}.trimmed.fastq
        #         # fastqc -o {rundir}/03_fastqc {rundir}/02_trimmed/{sample_filename}.trimmed.fastq.gz
        #         # """)
        #     tasks.append(current_t)

    # multi_fastqc = pipeline.create_job(
    #     name="03.5_multiqc_{sample_name}",
    #     dependences=tasks,
    #     local_arguments=qp.Arguments(
    #         sample_name=data["Sample_Name"],
    #         fastq_filenames=",".join(fastq_filenames)))

    # multi_fastqc.async_run("""
    #     module load python
    #     module load multiqc/1.2

    #     multiqc {rundir}/03_fastqc/ \
    #     --interactive \
    #     -o {rundir}/03.5_multifastqc/
    #     """)

        # Run tophat
        # tophat_t = pipeline.create_job(
        #     name="02_tophat_{sample_name}",
        #     dependences=tasks,
        #     local_arguments=qp.Arguments(
        #         sample_name=data["Sample_Name"],
        #         fastq_filenames=",".join(fastq_filenames)))

        tophat_t = pipeline.create_job(
            name="02_tophat_{sample_name}",
            local_arguments=qp.Arguments(
                sample_name=data["Sample_Name"],
                fastq_filenames=",".join(fastq_filenames)))

        tophat_t.async_run("""
            module load tophat/2.1.0
            module load samtools
            module load bowtie2/2.2.6
            module load boost
            module load gcc/4.8.3
            module load python
            module load cufflinks/2.2.1

            cufflinks \
                --multi-read-correct \
                -g {quantification_transcriptome_index} \
                -p {num_processors} \
                -o {rundir}/04_alignment/{sample_name}/ \
                {rundir}/04_alignment/{sample_name}/accepted_hits.bam
            """)

            # tophat --no-novel-juncs --report-secondary-alignments \
            # --read-mismatches {tophat_read_mismatches} \
            # --read-edit-dist {tophat_read_edit_dist} \
            # --num-threads {num_processors} \
            # --max-multihits {tophat_max_multihits} \
            # --transcriptome-index {tophat_transcriptome_index} \
            # -o {rundir}/04_alignment/{sample_name} \
            # {tophat_bowtie_index} \
            # {fastq_filenames}

            # ln -s {rundir}/04_alignment/{sample_name}/accepted_hits.bam {rundir}/04_alignment/{sample_name}.bam

            # samtools index {rundir}/04_alignment/{sample_name}.bam
            # samtools view -h -o {rundir}/04_alignment/{sample_name}/read_info.sam \
            # {rundir}/04_alignment/{sample_name}/accepted_hits.bam

            # htseq-count -f bam -q -m intersection-nonempty \
            #     -s reverse -t exon -i gene_id \
            #     {rundir}/04_alignment/{sample_name}.bam \
            #     {quantification_transcriptome_index} \
            #     > {rundir}/04_alignment/{sample_name}.htseq.counts
            # """)
        # step3_tasks.append(tophat_t)

    # t4 = pipeline.create_job(
    #     name="03_alignment_report",
    #     dependences=step3_tasks)
    # t4.async_run("""
    #     module load R/3.3.1

    #     Rscript /projects/p20742/tools/createTophatReport.R --topHatDir={rundir}/04_alignment/ --nClus={num_processors}
    #     """)

    # t5 = pipeline.create_job(
    #     name="04_quantification",
    #     dependences=[t4])
    # t5.async_run("""
    #     perl /projects/p20742/tools/makeHTseqCountsTable.pl {rundir}/04_alignment \
    #         {quantification_transcriptome_index} \
    #         {rundir}/05_quantification
    #     rm -f {project_dir}/latest
    #     ln -s {rundir} {project_dir}/latest
    #     """)

    # t1.unhold()

    state_filename = pipeline.save_state("{rundir}/pipeline.json")

# qp.wait_for_pipeline(state_filename)
