from questmon import Pipeline, MJobStatus
from os.path import expanduser

pipeline = Pipeline.load_state(expanduser("~/pipeline.json"))
result = pipeline.abort()
pipeline.close()
