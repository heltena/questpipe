from questmon import Pipeline, MJobStatus
from os.path import expanduser

pipeline = Pipeline.load_state(expanduser("~/pipeline.json"))
result = pipeline.checkjobs()
print("Completed: {}".format(result.get(MJobStatus.COMPLETED, 0)))
print("Running:   {}".format(result.get(MJobStatus.RUNNING, 0)))
print("Idles:     {}".format(result.get(MJobStatus.CREATED, 0)))
pipeline.close()
