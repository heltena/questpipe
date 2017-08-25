from datetime import datetime
from questmon import Arguments
from questmon.helpers import wait_for_pipeline
from pulrseq_pipeline import run_pipeline

import sys

projects = [
    ('160728_NB501488_0018_AHFJJVBGXY', '160728_AM'), 
    ('170323_NB501488_0080_AHVV5GBGXY', '170323_Liver'), 
    ('160730_NB501488_0019_AHCK5NBGXY', '160730_Lung'), 
    ('170329_NB501488_0082_AH2VV5BGX2', '170329_Adrenal'), 
    ('160801_NB501488_0020_AHW7KJBGXX', '160801_AM'), 
    ('170330_NB501488_0083_AHFJY7BGX2', '170330_Esophagus'), 
    ('160803_NB501488_0022_AHFWHLBGXY', '160730_Lung_rerun'), 
    ('170331_NB501488_0084_AHM2LJBGX2', '170331_Stomach'), 
    ('160804_NB501488_0023_AHFY22BGXY', '160730_AT'), 
    ('170405_NB501488_0086_AHMG5YBGX2', '170405_DB_ATAM'), 
    ('160805_NB501488_0024_AHFY5VBGXY', '160730_AT_rerun'), 
    ('170412_NB501488_0092_AHLYWVBGX2', '170410_SI'), 
    ('160811_NB501488_0025_AHFY2GBGXY', '160811_Blood'), 
    ('170413_NB501488_0093_AH5KGFBGX2', '170411_LI'), 
    ('160817_NB501488_0026_AHHL7FBGXY', '160812_MoDC'), 
    ('170414_NB501488_0094_AH5FYKBGX2', '170412_BAT'), 
    ('160826_NB501488_0029_AHGH72BGXY', '160826_Brain'), 
    ('170420_NB501488_0097_AHFFHNBGX2', '170422_WAT'), 
    ('160902_NB501488_0030_AHG7JMBGXY', '160902_Brain'), 
    ('170421_NB501488_0098_AHFG5MBGX2', '170423_DB_WL'), 
    ('160903_NB501488_0031_AHHCYFBGXY', '160903_Cerebellum'), 
    ('170424_NB501488_0099_AHCLK7BGX2', '170424_LI'), 
    ('160909_NB501488_0033_AHG723BGXY', '160909_Heart'), 
    ('170531_NB501488_0114_AHT2FVBGX2', '170531_Skin'), 
    ('160922_NB501488_0038_AHLMWCBGXY', '160922_Kidney'), 
    ('170717_NB501488_0130_AH5T2CBGX3', '170717_MuscSat'), 
    ('161025_NB501488_0047_AHVLGJBGXY', '161025_GutEP')]

from_index = 0
to_index = len(projects)

if len(sys.argv) > 1:
    from_index = int(sys.argv[1])

if len(sys.argv) > 2:
    to_index = int(sys.argv[2])


MAX_PIPELINES = 4
waiting_list = []
for project_name, project_id in projects[from_index:to_index]:
    print("Running project {}...".format(project_name))

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

        basedir="/projects/b1038/Pulmonary/Workspace/mouse_aging_map/users/general/mock_questmon2",
        project_id=project_id,
        project_name=project_name,
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

    state_filename = run_pipeline(run_name, arguments=arguments)
    waiting_list.append(state_filename)
    while len(waiting_list) >= MAX_PIPELINES:
        current = waiting_list.pop(0)
        print("Waiting for project {}...".format(current))
        wait_for_pipeline(current)

while len(waiting_list) >= 0:
    current = waiting_list.pop(0)
    print("Waiting for project {}...".format(current))
    wait_for_pipeline(current)
