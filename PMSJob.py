import os
import json
from enum import Enum

class IOType(Enum):
    local = 0
    xrootd = 1

class PMSJob:
    def __init__(self):
        self.job = {
            "exe_args": [],
            "input": {
                "files": []
            },
            "output": None,
            "tags": [],
        }


    # sets the user
    def SetUser(self, user):
        self.job["user"] = user
        
    # set the executable
    def SetExecutable(self, exe):
        self.job["executable"] = exe
        
    # add a setenv script
    def AddSetenvScript(self, setenv):
        if not "env" in self.job:
            self.job["env"] = {}

        self.job["env"]["type"] = "script"
        self.job["env"]["file"] = setenv


    def AddInputTransfer(self, protocol, filename, source):
        self.job["input"]["files"].append({
            "protocol": protocol.name,
            "file": outfilename,
            "source": source
        })


    def AddOutputTransferWithTag(self, protocol, filename, destination, tag):
        if self.job["output"] != None and not isinstance(self.job["output"], list):
            print("Error: non-tagged file transfer already set up for this job")
            exit

        if self.job["output"] == None:
            self.job["output"] = []

        matched_items = [item for item in self.job["output"] if item["tag"] == tag]

        if len(matched_items) > 1:
            raise RuntimeError("Two output file transfers with the same tag. Should not happen.")
        elif len(matched_items) == 0:
            self.job["output"].append({
                "tag": tag,
                "files": [{
                    "protocol": protocol.name,
                    "file": filename,
                    "destination": destination
                }]
            })
        # we have exactly one match
        else:
            matched_items[0]["files"].append({
                "protocol": protocol.name,
                "file": filename,
                "destination": destination
            })

    def AddOutputTransfer(self, protocol, filename, destination):
        if self.job["output"] == None:
            self.job["output"] = {
                "files": []
            }

        self.job["output"]["files"].append({
            "protocol": protocol.name,
            "file": filename,
            "destination": destination
        })

        
    # set files for stdout and stderr
    def SetJobIO(self, name, destination):
        self.job["stdin"] = ""
        self.job["stdout"] = f"{name}.out.log"
        self.job["stderr"] = f"{name}.err.log"

        if destination.startswith("root://"):
            protocol = "xrootd"
        else:
            protocol = "local"
           
        self.job["output"]["files"].append({
            "protocol": protocol,
            "file": "*.log",
            "destination": destination
        })

    def SetJobName(self, jobname):
        self.job["jobName"] = jobname
    
    # add a flag to the executable
    def AddFlag(self, flag):
        if not "exe_args" in self.job:
            self.job["exe_args"] = []
            
        self.job["exe_args"].append(flag)

    def AddTags(self, *args):
        self.job["tags"] += [tag for tag in args]

    # add a generic key to the job description
    # NB: use only if you really need it
    def AddGenericKey(self, key, value):
        self.job[key] = value
        
    def AsDict(self):
        return self.job

    def AsJson(self):
        return json.dumps(self.job, indent=2)
    
