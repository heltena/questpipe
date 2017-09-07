import questpipe as qp
from os.path import expanduser
import sys

pipeline_name = sys.argv[1]
pipeline = qp.Pipeline.load_state(pipeline_name)
queue_count, running_count, completed_count = pipeline.checkjobs()
print("Completed: {}".format(completed_count))
print("Running:   {}".format(running_count))
print("Idles:     {}".format(queue_count))
