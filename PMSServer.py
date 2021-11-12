import asyncio
import json
import websockets

from PyMS.PMSExceptions import *

class PMSTask:
    def __init__(self, name, token):
        self.name = name
        self.token = token

class PMSServer:
    def __init__(self, address):
        self.websocket = None
        self.address = address
        self.uri = f"ws://{address}/server"
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.__async__connect())        
        pass

    def __del__(self):
        self.loop.run_until_complete(self.__async__close_connection())
    
    ## function to create a new task.
    ## Parameters:
    ##   (string) taskname
    def CreateTask(self, taskname):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "createTask",
            "task": taskname
        }) )
        
        if resp.startswith("Task"):
            return PMSTask(taskname, resp.split(" ")[-1])
        else:
            raise TaskOperationFailed(resp)
        
    ## function to delete an existing task.
    ## Parameters:
    ##   (PMSTask) task
    def ClearTask(self, task):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "clearTask",
            "task": task.name,
            "token": task.token
        }) )
        
        if resp.startswith("Task"):
            return resp
        else:
            raise TaskOperationFailed(resp)
    
    ## function to remove all jobs from an existing task.
    ## Parameters:
    ##   (PMSTask) task
    def CleanTask(self, task):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "cleanTask",
            "task": task.name,
            "token": task.token
        }) )
        
        if resp.startswith("Task"):
            return resp
        else:
            raise TaskOperationFailed(resp)
    
    
    ## function to declare a dependency between tasks
    ## Parameters:
    ##   (PMSTask) task
    ##   (string) dependsOn
    def DeclareTaskDependency(self, task, dependsOn):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "declareTaskDependency",
            "task": task.name,
            "token": task.token,
            "dependsOn": dependsOn
        }) )
        
        if resp.startswith("Task"):
            return resp
        else:
            raise TaskOperationFailed(resp)


    ## function to check validity of a task/token pair
    ## Parameters:
    ##   (PMSTask) task
    def ValidateTaskToken(self, task):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "validateTaskToken",
            "task": task.name,
            "token": task.token,
        }) )
        
        if resp.startswith("Task/token"):
            return True
        elif resp.startswith("Invalid"):
            return False
        else:
            raise TaskOperationFailed(resp)


    ## function to submit a new job in an existing task.
    ## Parameters:
    ##   (PMSJob) job
    ##   (PMSTask) task
    ## Returns:
    ##   (string) job hash
    def SubmitJob(self, job, task):
        resp = self.loop.run_until_complete( self.send_to_orchestrator({
            "command": "submitJob",
            "job": job.job,
            "task": task.name,
            "token": task.token
        }, False) )
        
        if resp.startswith("Job received"):
            return resp.split(" ")[-1]
        else:
            raise JobOperationFailed(resp)
        
    async def __async__connect(self):
        self.websocket = await websockets.connect(self.uri)
        
    async def __async__close_connection(self):
        await self.websocket.close()
        
    async def send_to_orchestrator(self, msg, verbose = True):
        await self.websocket.send(json.dumps(msg))
    
        response = await self.websocket.recv()
        if verbose:
            print(f"PMS Server replied: {response}")
        return response
