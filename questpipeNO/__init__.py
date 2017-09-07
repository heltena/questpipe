class Arguments:
    def __init__(self, **kwargs):
        self.values = kwargs

    @staticmethod
    def from_json(data):
        return Arguments(**data)

    def get(self, key, default_value):
        return self.values.get(key, default_value)

    def to_json(self):
        return self.values

    def combine(self, other):
        values = self.values.copy()
        if other is not None:
            for k, v in other.values.items():
                values[k] = v 
        return Arguments(**values)

    def __repr__(self):
        return self.values.__repr__()   


class SecureShell:
    def __init__(self, hostname, user, key_filename):
        pass


class Job:
    CREATED = 0
    RUNNING = 1
    COMPLETED = 2

    def __init__(self, pipeline, name, msub_arguments, dependences, notokdependences,
                 workdir, outdir, errdir, arguments, moab_job_name, moab_job_id, 
                 status, command):
        self.pipeline = pipeline
        self.name = name
        self.msub_arguments = msub_arguments
        self.dependences = dependences
        self.notokdependences = notokdependences
        self.workdir = workdir
        self.outdir = outdir
        self.errdir = errdir
        self.arguments = arguments
        self.moab_job_name = moab_job_name
        self.moab_job_id = moab_job_id
        self.status = status
        self.command = command

    @staticmethod
    def create_new(pipeline, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, arguments):
        return Job(pipeline, name, msub_arguments, dependences, notokdependences, workdir, outdir, errdir, arguments, None, None, Job.CREATED, None)

    @staticmethod
    def from_json(pipeline, msub_arguments, data):
        return MJob(
            pipeline, 
            data["name"], 
            msub_arguments,
            [],   # fix dependences later
            [],   # fix notok dependences later
            data["workdir"],
            data["outdir"],
            data["errdir"],
            pipeline.arguments,
            data["moab_job_name"],
            data["moab_job_id"],
            data["status"],
            data["command"])

    def to_json(self):
        result = {
            "name": self.name,
            "workdir": self.workdir,
            "outdir": self.outdir,
            "errdir": self.errdir,
            "moab_job_name": self.moab_job_name,
            "moab_job_id": self.moab_job_id,
            "status": self.status,
            "command": self.command}
        if self.dependences is not None:
            result["dependences"] = [job.moab_job_id for job in self.dependences]
        if self.notokdependences is not None:
            result["notokdependences"] = [job.moab_job_id for job in self.notokdependences]
        return result

    def __parse_string(self, value, max_recursive_loops=10):
        for i in range(max_recursive_loops):
            new_value = value.format(job_name=self.name, **self.arguments.values)
            if new_value != value:
                value = new_value
            else:
                return new_value
        raise Exception("recursive parsing")

    def prepare_async_run(self, command, NUMBER_OF_ATTEMPTS=3):
        self.__async_run(command, hold=True, NUMBER_OF_ATTEMPTS=NUMBER_OF_ATTEMPTS)

    def async_run(self, command, NUMBER_OF_ATTEMPTS=3):
        self.__async_run(command, hold=False, NUMBER_OF_ATTEMPTS=NUMBER_OF_ATTEMPTS)

    def __async_run(self, command, hold, NUMBER_OF_ATTEMPTS=3):
        if self.status != MJob.CREATED:
            raise Exception("Job is running")
        self.command = command
        eff_command = self.__parse_string(command)
        eff_msub_arguments = [self.__parse_string(arg) for arg in self.msub_arguments]
        if self.dependences is not None and len(self.dependences) > 0:
            self.pipeline.log("I: dep: {}".format([job.moab_job_id for job in self.dependences]))
            for mjob in self.dependences:
                if mjob.moab_job_id is None:
                    raise Exception("Job must be running in order to be dependence")
            moab_job_ids = [mjob.moab_job_id for mjob in self.dependences]
            eff_msub_arguments.append("-l depend=afterok:{}".format(":".join(moab_job_ids)))

        if self.notokdependences is not None and len(self.notokdependences) > 0:
            self.pipeline.log("I: notok dep: {}".format([job.moab_job_id for job in self.notokdependences]))
            for mjob in self.notokdependences:
                if mjob.moab_job_id is None:
                    raise Exception("Job must be running in order to be not ok dependence")
            moab_job_ids = [mjob.moab_job_id for mjob in self.notokdependences]
            eff_msub_arguments.append("-l depend=afternotok:{}".format(":".join(moab_job_ids)))

        workdir = self.__parse_string(self.workdir)
        errdir = self.__parse_string(self.errdir)
        outdir = self.__parse_string(self.outdir)

        eff_msub_arguments.append("-d \"{}\"".format(workdir))
        eff_msub_arguments.append("-e \"{}\"".format(errdir))
        eff_msub_arguments.append("-o \"{}\"".format(outdir))
        
        if hold:
            eff_msub_arguments.append("-h")
    
        for i in range(NUMBER_OF_ATTEMPTS):
            self.pipeline.log("I: msub attempt {}: {}".format(i + 1, eff_msub_arguments))
            stdin, stdout, stderr = self.pipeline.exec_command("msub", eff_msub_arguments, input=eff_command)
            result = len(stderr) == 0
            if result:
                self.moab_job_name = stdout.decode('utf8').strip()
                self.moab_job_id = self.moab_job_name.split(".")[0]
                self.status = MJob.RUNNING
                self.pipeline.log("I: Running {}".format(self.moab_job_id))
                break
        else:
            # when for exit is not because break
            raise Exception("Cannot start job after {} attemps: {}".format(NUMBER_OF_ATTEMPTS, stderr))
        return self

    def unhold(self):
        self.pipeline.log("I: unholding {}".format(self.moab_job_id))
        stdin, stdout, stderr = self.pipeline.exec_command("mjobctl", ["-u", "all", self.moab_job_id])
        result = len(stderr) == 0
        if result:
            self.status = Job.RUNNING
            self.pipeline.log("I: Unhold {}".format(self.moab_job_id))
        else:
            self.pipeline.log("E: Error unholding job {}: {}".format(self.moab_job_id, stderr))
            self.status = Job.CREATED

    @property
    def is_running(self):
        if self.status in [Job.CREATED, Job.COMPLETED]:
            return False
        stdin, stdout, stderr = self.pipeline.exec_command("checkjob", ["-v {}".format(self.moab_job_id)])
        for line in stdout.splitlines():
            line = line.decode("utf8")
            if line.endswith('\n'):
                line = line[:-1]
            index = line.find(':')
            if index == -1:
                continue
            name = line[0:index].strip()
            if name != 'State':
                continue
            param = line[index+1:].strip()
            if param == "Completed":
                self.status = Job.COMPLETED
                return False
            else:
                return True
        return False 


class Pipeline:
    def __init__(self, name, shell, arguments, debug_filename=None, state_filename=None):
        self.name = name
        self.shell = shell
        self.arguments = arguments
        self.debug_filename = debug_filename
        if self.debug_filename is not None:
            self.debug_file = open(self.debug_filename, "wt")
        else:
            self.debug_file = None
        self.state_filename = state_filename
    
    @staticmethod
    def load_state(filename, shell):
        with open(filename, "rt") as f:
            data = json.loads(f.read())
        return Pipeline.from_json(data, shell)

    @staticmethod
    def from_json(data, shell):
        name = data["name"]
        arguments = Arguments.from_json(data["arguments"])
        msub_arguments = arguments.get("msub_arguments", [])
        pipeline = Pipeline(name, shell, arguments)
        jobs = [(d.get("dependences", []), d.get("notokdependences", []), Job.from_json(pipeline, msub_arguments, d)) for d in data["jobs"]]
        job_ids = {job.moab_job_id: job for _, _, job in jobs}
        for dependences, notokdependences, job in jobs:
            job.dependences = [job_ids[dependence] for dependence in dependences]
            job.notokdependences = [job_ids[dependence] for dependence in notokdependences]
        pipeline.jobs = [job for (dependence, notokdependence, job) in jobs]
        return pipeline

    def abort(self):
        pass

    def create_job(self, name, dependences=[]):
        return Job()

    def log(self, text):
        pass

    def show_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.debug_file is not None:
            self.debug_file.close()
        if exc_type is not None:
            self.log("E: Aborting all the jobs (exception raised)")
            self.abort()
            return None
        return self
