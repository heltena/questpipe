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

t0 = pipeline.create_job(name="createdir")
t0.async_run("""
mkdir -p 01_fastq
mkdir 02_fastqc
mkdir 03_fastqc_trimmed
mkdir 04_alignment
mkdir 05_quantify
""")

t1 = pipeline.create_job(name="fastqc")
t1.async_run
    fastqc  -o 01_fastqc 
    """)

#t2 = pipeline.create_job(name="trimming")
#t2.async_run("""

 #   Java -jar Trimmomatic -----
#    echo {job_name} >> helio001
#    """)
#t3 = pipeline.create_job(name="fastqc_post")
#t3.async_run("""
#    sleep 10
#    echo {job_name} >> helio001
#    """)

#t4 = pipeline.create_job(name="alignment")
#t4.async_run("""
#    sleep 10
#    echo {job_name} >> helio001
#    """)
#t5 = pipeline.create_job(name="quantify")
#t5.async_run("""
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
