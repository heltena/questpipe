from questmon import Pipeline, MJob
from os.path import expanduser
import sys

pipeline_name = sys.argv[1]
pipeline = Pipeline.load_state(pipeline_name)
result = pipeline.checkjobs()
print("Completed: {}".format(result.get(MJob.COMPLETED, 0)))
print("Running:   {}".format(result.get(MJob.RUNNING, 0)))
print("Idles:     {}".format(result.get(MJob.CREATED, 0)))
pipeline.close()
