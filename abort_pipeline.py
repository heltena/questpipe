import questpipe as qp
from os.path import expanduser
import sys

pipeline_name = sys.argv[1]
pipeline = qp.Pipeline.load_state(pipeline_name)
result = pipeline.abort()
pipeline.close()
