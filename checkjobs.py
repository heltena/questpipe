from questmon import Pipeline, MJobStatus
from os.path import expanduser
import sys

pipeline_name = sys.argv[1]
pipeline = Pipeline.load_state(pipeline_name)
result = pipeline.checkjobs()
print("Completed: {}".format(result.get(MJobStatus.COMPLETED, 0)))
print("Running:   {}".format(result.get(MJobStatus.RUNNING, 0)))
print("Idles:     {}".format(result.get(MJobStatus.CREATED, 0)))
pipeline.close()
