import csv
import json
import re
import subprocess


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


class MJobStatus:
    CREATED = 0
    RUNNING = 1
    COMPLETED = 2


class MJob:
    def __init__(self, pipeline, name, msub_arguments, dependences, workdir, outdir, errdir, arguments, moab_job_name, moab_job_id, status, command):
        self.pipeline = pipeline
        self.name = name
        self.msub_arguments = msub_arguments
        self.dependences = dependences
        self.workdir = workdir
        self.outdir = outdir
        self.errdir = errdir
        self.arguments = arguments
        self.moab_job_name = moab_job_name
        self.moab_job_id = moab_job_id
        self.status = status
        self.command = command

    @staticmethod
    def create_new(pipeline, name, msub_arguments, dependences, workdir, outdir, errdir, arguments):
        return MJob(pipeline, name, msub_arguments, dependences, workdir, outdir, errdir, arguments, None, None, MJobStatus.CREATED, None)

    @staticmethod
    def from_json(pipeline, msub_arguments, data):
        return MJob(
            pipeline, 
            data["name"], 
            msub_arguments,
            [],   # fix dependences later
            data["workdir"],
            data["outdir"],
            data["errdir"],
            pipeline.arguments,
            data["moab_job_name"],
            data["moab_job_id"],
            data["status"],
            data["command"])

    def to_json(self):
        return {
            "name": self.name,
            "dependences": [job.moab_job_id for job in self.dependences],
            "workdir": self.workdir,
            "outdir": self.outdir,
            "errdir": self.errdir,
            "moab_job_name": self.moab_job_name,
            "moab_job_id": self.moab_job_id,
            "status": self.status,
            "command": self.command}

    def __parse_string(self, input):
        return input.format(job_name=self.name, **self.arguments.values)

    def async_run(self, command):
        if self.status != MJobStatus.CREATED:
            raise Exception("MJob is running")
        self.command = command
        eff_command = self.__parse_string(command)
        eff_msub_arguments = [self.__parse_string(arg) for arg in self.msub_arguments]
        if self.dependences is not None and len(self.dependences) > 0:
            print("dep: {}".format(self.dependences))
            for mjob in self.dependences:
                if mjob.moab_job_id is None:
                    raise Exception("MJob must be running in order to be dependence")
            moab_job_ids = [mjob.moab_job_id for mjob in self.dependences]
            eff_msub_arguments.append("-W depend=afterok:{}".format(":".join(moab_job_ids)))

        workdir = self.__parse_string(self.workdir)
        errdir = self.__parse_string(self.errdir)
        outdir = self.__parse_string(self.outdir)

        eff_msub_arguments.append("-d \"{}\"".format(workdir))
        eff_msub_arguments.append("-e \"{}\"".format(errdir))
        eff_msub_arguments.append("-o \"{}\"".format(outdir))
        
        print("I: msub {}".format(eff_msub_arguments))
        stdin, stdout, stderr = self.pipeline.exec_command("msub", eff_msub_arguments, input=eff_command)
        result = len(stderr) == 0
        if result:
            self.moab_job_name = stdout.decode('utf8').strip()
            self.moab_job_id = self.moab_job_name.split(".")[0]
            self.status = MJobStatus.RUNNING
            print("I: Running {}".format(self.moab_job_id))
        else:
            self.moab_job_name = None
            self.moab_job_id = None
            self.status = MJobStatus.CREATED
            print("E: {}".format(stderr))
        return self

    @property
    def is_running(self):
        if self.status in [MJobStatus.CREATED, MJobStatus.COMPLETED]:
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
                self.status = MJobStatus.COMPLETED
                return False
            else:
                return True
        return False 


class Pipeline:
    def __init__(self, name, join_command_arguments=False, arguments=None):
        self.name = name
        self.join_command_arguments = join_command_arguments
        self.arguments = arguments if arguments is not None else Arguments({})
        self.jobs = []

    @staticmethod
    def load_state(filename):
        with open(filename, "rt") as f:
            data = json.loads(f.read())
        return Pipeline.from_json(data)

    @staticmethod
    def from_json(data):
        name = data["name"]
        join_command_arguments = data["join_command_arguments"]
        arguments = Arguments.from_json(data["arguments"])
        msub_arguments = arguments.get("msub_arguments", [])
        pipeline = Pipeline(name, join_command_arguments, arguments)
        jobs = [(d["dependences"], MJob.from_json(pipeline, msub_arguments, d)) for d in data["jobs"]]
        job_ids = {job.moab_job_id: job for _, job in jobs}
        for dependences, job in jobs:
            job.dependences = [job_ids[dependence] for dependence in dependences]
        pipeline.jobs = [job for (dependence, job) in jobs]
        return pipeline

    def to_json(self):
        return {
            "name": self.name,
            "join_command_arguments": self.join_command_arguments,
            "arguments": self.arguments.to_json(),
            "jobs": [job.to_json() for job in self.jobs]}

    def save_state(self, filename):
        with open(filename, "wt") as f:
            f.write(json.dumps(
                self.to_json(), 
                sort_keys=True, 
                indent=4, 
                separators=(',', ': ')))

    def checkjobs(self):
        # If the job is not appearing on 'showq', it means, the job is done
        job_states = {job.moab_job_id: MJobStatus.COMPLETED for job in self.jobs}  
        p = subprocess.Popen("showq", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        for line in stdout.splitlines():
            line = line.decode("utf8")
            if line.endswith('\n'):
                line = line[:-1]
            line = line.strip()
            if len(line) == 0:
                continue
            params = re.sub('\s+', ' ', line).split()
            if params[0] in job_states:
                state = params[2].upper()
                if state == "RUNNING":
                    job_states[params[0]] = MJobStatus.RUNNING
                else:
                    job_states[params[0]] = MJobStatus.CREATED

        count = {}
        for state in job_states.values():
            count[state] = count.get(state, 0) + 1
        return count

    def abort(self):
        job_states = {job.moab_job_id: MJobStatus.COMPLETED for job in self.jobs}  
        for job in self.jobs:
            p = subprocess.Popen("mjobctl -c {}".format(job.moab_job_id), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()

    def exec_command(self, command, command_arguments, input=None):
        if command_arguments is None:
            command_arguments = []
            
        if self.join_command_arguments:
            eff_command = " ".join([command] + command_arguments)
        else:
            eff_command = [command] + command_arguments

        print("I: {} / {}".format(eff_command, input if input is not None else "None"))
        if input is not None:
            p = subprocess.Popen(eff_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate(bytes(input, "utf-8"))
            return None, stdout, stderr
        else:
            p = subprocess.Popen(eff_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            return None, stdout, stderr

    def create_job(self, name, local_arguments=None, dependences=None, workdir=None, outdir=None, errdir=None):
        self.arguments = self.arguments.combine(local_arguments)
        msub_arguments = self.arguments.get("msub_arguments", [])
        if dependences is None:
            dependences = []
        if workdir is None:
            workdir = self.arguments.get("workdir", ".")
        if outdir is None:
            outdir = self.arguments.get("outdir", ".")
        if errdir is None:
            errdir = self.arguments.get("errdir", ".")
        job = MJob.create_new(self, name, msub_arguments, dependences, workdir, outdir, errdir, self.arguments)
        self.jobs.append(job)
        return job

    def parse_string(self, input):
        return input.format(**self.arguments.values)

    def run(self, command):
        eff_command = self.parse_string(command)
        p = subprocess.Popen(eff_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        return None, stdout, stderr
        
    def close(self):
        pass


class SampleSheetLoader:
    BEGIN = 0
    HEADER = 1
    READS = 2
    SETTINGS = 3
    DATA_HEADER = 4
    DATA = 5

    def __init__(self, filename):
        state = SampleSheetLoader.BEGIN
        headers = {}
        reads = {}
        settings = {}
        data_header = []
        data_values = []
        with open(filename, "r") as f:
            for params in csv.reader(f):
                if params[0] == "[Header]":
                    state = SampleSheetLoader.HEADER
                    continue
                if params[0] == "[Reads]":
                    state = SampleSheetLoader.READS 
                    continue
                if params[0] == "[Settings]":
                    state = SampleSheetLoader.SETTINGS 
                    continue
                if params[0] == "[Data]":
                    state = SampleSheetLoader.DATA_HEADER
                    continue
                if state == SampleSheetLoader.HEADER:
                    if len(params[0].strip()) > 0:
                        headers[params[0]] = params[1:]
                elif state == SampleSheetLoader.READS:
                    if len(params[0].strip()) > 0:
                        reads[params[0]] = params[1:]
                elif state == SampleSheetLoader.SETTINGS:
                    if len(params[0].strip()) > 0:
                        settings[params[0]] = params[1:]
                elif state == SampleSheetLoader.DATA_HEADER:
                    data_header = params
                    state = SampleSheetLoader.DATA
                elif state == SampleSheetLoader.DATA:
                    data_values.append(params)
                else:
                    print("Error!")
        self.data_header = data_header
        self.data_values = data_values
        self.data = [dict(zip(self.data_header, values)) for values in self.data_values]
    


# class SSHShell:
#     def __init__(self, host, username, key):
#         self.client = paramiko.SSHClient()
#         self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         self.client.connect(host, username=username, pkey=key)
    
#     def exec_command(self, command, arguments, input=None):
#         arguments = " ".join(arguments)
#         if input is None:
#             eff_command = "{} {}".format(command, arguments)
#             _, stdout, stderr = self.client.exec_command(eff_command)
#             return _, stdout.read(), stderr.read()
#         else:
#             eff_command = "echo $'{}' | {} {}".format(input, command, arguments)
#             _, stdout, stderr = self.client.exec_command(eff_command)
#             return _, stdout.read(), stderr.read()

#    def create_job(self, msub_arguments, dependences, command, workdir=None, outdir=None, errdir=None):
#             raise NotImplemented()   

#     def close(self):
#         self.client.close()
