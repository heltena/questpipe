from datetime import datetime
from questmon import Arguments
from questmon.helpers import wait_for_pipeline
from pulrseq_pipeline import run_pipeline


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

    basedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_questmon",
    project_id="160728_AM",
    project_name="160728_NB501488_0018_AHFJJVBGXY",
    project_dir="{basedir}/{project_name}",

    rundir="{project_dir}/{run_name}",
    illumina_csv_sheet="{project_dir}/SampleSheet.csv",

    tophat_read_mismatches=2,
    tophat_read_edit_dist=2,
    tophat_max_multihits=5,
    tophat_transcriptome_index="/projects/p20742/anno/tophat_tx/mm10.Ens_78.cuff",  # tophat uses GFF files and it does not like the extension :-(
    tophat_bowtie_index="/projects/p20742/anno/bowtie_indexes/mm10",
    quantification_transcriptome_index="/projects/p20742/anno/Ens/mm10.Ens_78/mm10.Ens_78.cuff.gtf",

    workdir="{project_dir}/",
    outdir="{rundir}/logs/",
    errdir="{rundir}/logs/",
)

state_filename = run_pipeline("pulrseq", arguments=arguments)
wait_for_pipeline(state_filename)
