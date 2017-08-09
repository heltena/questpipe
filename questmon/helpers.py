from . import Pipeline


def wait_for_pipeline(state_filename):
    pipeline = Pipeline.load_state(state_filename)
    while True:
        time.sleep(60)
        result = pipeline.checkjobs()
        completed = result.get(MJob.COMPLETED, 0)
        running = result.get(MJob.RUNNING, 0)
        created = result.get(MJob.CREATED, 0)
        if running == 0 and created == 0:
            pipeline.log("I: Completed!")
            break