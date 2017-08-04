from questmon import Pipeline, MJobStatus
from os.path import expanduser
import sys

pipeline_name = sys.argv[1]
pipeline = Pipeline.load_state(pipeline_name)
result = pipeline.abort()
pipeline.close()
