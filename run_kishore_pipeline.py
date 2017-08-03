from questmon import Arguments, Pipeline
from os.path import expanduser


arguments = Arguments(
    msub_arguments=[
        "-A b1042 ",
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
# directories created 

t1 = pipeline.create_job(name="fastqc")
t1.async_run("""
    module load fastqc/0.11.5
    fastqc {input_fastq.gz file} -o ~/01_fastqc 
    """)

t2 = pipeline.create_job(name="trimming")
# if required the new trimmomatic tools can be adopted 
# resulting files need to be moved to trimmed folder
t2.async_run("""
   module load java
   java -jar /projects/b1038/tools/Trimmomatic-0.36/trimmomatic-0.36.jar SE -threads <numprocessors> -phred33 <fastqfile/location> <fastqfile/rename/ifrequired>
   TRAILING:30 MINLEN:20
   gzip <location/of/fastqfile>
    """)
t3 = pipeline.create_job(name="fastqc_trimmed")
t3.async_run("""
     module load fastqc/0.11.5
     fastqc <input_fastq.gz file> -o ~/03_fastqc_trimmed

#    """)

#t4 = pipeline.create_job(name="alignment")
#t4.async_run("""
#    sleep 10
#    echo {job_name} >> helio001
#    """)

# this step takes longer time # may be we can make it independent of any other down stream steps
# we also get a log file for this job so it can be directed to logs directory created
t5 = pipeline.create_job(name="GBodycov")
t5.async_run("""
    module load samtools/1.2 
    module load R/3.3.3
    module load python # in which rseqc was integrated
    #
    refbedfile=/projects/b1038/Pulmonary/Anno/mm10/mm10_RefSeq.bed
    bamdirectory="/projects/b1038/Pulmonary/Workspace/<location/of/bamfiles>"
    cd $bamdirectory # should also contain the bai files
    mkdir -p RSeQCreport # can be restructured according to Questmon or we can add prefix directly with additional arguents
    geneBody_coverage.py -r $refbedfile -i $bamdirectory -o /projects/b1038/Pulmonary/Workspace/testbam/<location/of/RSeQCreport>
    """)



#t6 = pipeline.create_job(name="quantify")
#t6.async_run("""
#    sleep 10
#    echo {job_name} >> helio001
#    """)

#t3 = pipeline.create_job(name="summarize", dependences=[t1, t2])
#t3.async_run("""
#    sleep 1
#    echo {job_name} >> FINISHED
#    """)
    
pipeline.save_state(expanduser("~/pipeline.json"))
pipeline.close()
