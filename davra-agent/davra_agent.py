# Davra Device Agent
# This runs on a device so it communicates with the Davra Server.
# It facilitates running Applications on Devices and running Jobs.
# It should run continuously (as a service) on a device.
#
#
import subprocess
import os, sys, string
import time, requests, os.path
from requests.auth import HTTPBasicAuth
import json 
from pprint import pprint
import datetime
import paho.mqtt.client as mqtt
import ssl
import signal
import threading
import uuid
import hashlib
import davra_lib as comDavra
# If you add new libraries to the agent, update requirements.txt


# sys.dont_write_bytecode = True
# Confirm configuration available from within the config file
# Startup side effects are performed only by main(), not by an import.

# The job details of currently running job for this device.
# Files are used to track the currently running Job, script and function.
# Each get a dirctory to themselves and a json for tracking.
# The directory "currentJob" etc may be removed when job is finished.
currentJobDir = comDavra.installationDir + '/currentJob'
currentJobJson = currentJobDir + '/job.json'
currentFunctionDir = comDavra.installationDir + "/currentFunction"
currentFunctionJson = currentFunctionDir + "/currentFunction.json"
# Some flags to cache in RAM whether a function/script/job is running 
# rather than checking the hard drive continuously
flagIsFunctionRunning = False
flagIsScriptRunning = False
flagIsJobRunning = False
_finishingFunctions = set()
_jobReporting = False
_stopRequested = threading.Event()


def _functionUuid(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError("invalid_function_uuid")
    return value


def _functionPath(functionUuid):
    return os.path.join(currentFunctionDir, _functionUuid(functionUuid) + ".json")


def _functionRecords():
    paths = [currentFunctionJson]
    if os.path.isdir(currentFunctionDir):
        for name in os.listdir(currentFunctionDir):
            if len(name) == 41 and name.endswith(".json"):
                try:
                    paths.append(_functionPath(name[:-5]))
                except ValueError:
                    continue
    result = []
    with comDavra.stateLock:
        for path in paths:
            try:
                row = comDavra.readJsonFile(path)
                _functionUuid(row["functionParameterValues"]["functionUuid"])
                result.append((path, row))
            except FileNotFoundError:
                continue
            except (ValueError, KeyError, TypeError) as error:
                comDavra.logProblem('function_state', 'function_state_invalid', error_class=type(error).__name__)
    return result


def _recentFunctions():
    try:
        return comDavra.readJsonFile(os.path.join(currentFunctionDir, ".reported-functions.json"))
    except FileNotFoundError:
        return {}


def _functionFingerprint(name, parameters):
    return hashlib.sha256(json.dumps([name, parameters], sort_keys=True).encode()).hexdigest()


def _refreshFunctionFlag():
    global flagIsFunctionRunning
    flagIsFunctionRunning = bool(_functionRecords())


def sendHeartbeatMetricsToServer():
    dataToSend = [{ 
        "UUID": comDavra.conf['UUID'],
        "name": "uptime",
        "value": comDavra.getUptimeProcess(),
        "msg_type": "datum"
    }, { 
        "UUID": comDavra.conf['UUID'],
        "name": "cpu",
        "value": comDavra.getUptime()[1],
        "msg_type": "datum",
    }, { 
        "UUID": comDavra.conf['UUID'],
        "name": "ram",
        "value": comDavra.getRam()[1],
        "msg_type": "datum",
    }, { 
        "UUID": comDavra.conf['UUID'],
        "name": "davra.agent.heartbeat",
        "value": {
            "davraAgentVersion": comDavra.davraAgentVersion,
            "heartbeatInterval": comDavra.conf['heartbeatInterval'],
            "mqtt": mqttHealth()
        },
        "msg_type": "event"
    }]
    #print(json.dumps(dataToSend, indent=4))
    statusCode = comDavra.sendDataToServer(dataToSend).status_code
    return



###########################   JOBS

def checkForPendingJob():
    headers = comDavra.getHeadersForRequests()
    dataToSend = { 
        "deviceUUID": comDavra.conf['UUID'], 
        "deviceStatus": "pending",
        "jobStatus": "active",
        "oldest": True
    }
    r = comDavra.httpPut(comDavra.conf['server'] + '/api/v1/jobs', dataToSend)
    if (r.status_code == 200):
        pendingJobs = json.loads(r.content) if comDavra.isJson(r.content) else []
        if(pendingJobs != [] and len(pendingJobs) > 0):
            runDavraJob(pendingJobs[0])
        else:
            pass  # The shared HTTP path records the outcome.
        return
    else:
        return(r.status_code)
    

# For any type of job, determine which type (eg script) and run it
def runDavraJob(jobObject):
    global flagIsJobRunning
    # Catch situation where a job is already running
    with comDavra.stateLock:
        if flagIsJobRunning or os.path.isfile(currentJobJson):
            comDavra.logProblem('job_busy', 'command_rejected', 'WARN', job_uuid=jobObject['UUID'], reason='job_busy')
            return
        flagIsJobRunning = True
    comDavra.logEvent('job_received', job_uuid=jobObject['UUID'])
    try:
        # Write this job to disk as the current job
        jobObject['devices'][0]['startTime'] = comDavra.getMilliSecondsSinceEpoch()
        jobObject['devices'][0]['status'] = 'running'
        comDavra.provideFreshDirectory(currentJobDir)
        with open(currentJobJson, 'w') as outfile:
            json.dump(jobObject, outfile, indent=4)
        if ('jobConfig' in jobObject and jobObject['jobConfig']['type'].lower() == 'runfunction'):
            comDavra.logEvent('job_running', job_uuid=jobObject['UUID'], function=jobObject['jobConfig']['functionName'])
            runFunction(jobObject['jobConfig']['functionName'], jobObject['jobConfig']['functionParameterValues'], jobUuid=jobObject['UUID'])
            return
        # Reaching here means the job type was not recognised so that is a failed situation
        updateJobWithResult('failed', 'Unknown job type')
        checkCurrentJob()
        return
    except Exception as e:
        # Reaching here means the job type was not recognised so that is a failed situation
        comDavra.logProblem('job_run', 'agent_action_failed', job_uuid=jobObject.get('UUID'), error_class=type(e).__name__)
        updateJobWithResult('failed', 'Unknown job type')
        checkCurrentJob()
        return

        

# Check if the current job is finished
def checkCurrentJob():        
    global _jobReporting
    with comDavra.stateLock:
        if _jobReporting:
            return
        try:
            jobObject = comDavra.readJsonFile(currentJobJson)
        except FileNotFoundError:
            return
        if jobObject['devices'][0]['status'] not in ('completed', 'failed'):
            return
        _jobReporting = True
    try:
        reportJobStatus()
    finally:
        with comDavra.stateLock:
            _jobReporting = False
    return


def updateJobWithResult(status, responseText, expectedJobUuid=None):
    # Update the currentJob file on disk with the result. That will be noticed upon next "checkCurrentJob".
    with comDavra.stateLock:
        try:
            jobObject = comDavra.readJsonFile(currentJobJson)
        except FileNotFoundError:
            return
        if expectedJobUuid is not None and jobObject.get('UUID') != expectedJobUuid:
            return
        jobObject['devices'][0]['endTime'] = comDavra.getMilliSecondsSinceEpoch()
        jobObject['devices'][0]['status'] = status
        jobObject['devices'][0]['response'] = responseText
        comDavra.writeJsonAtomic(currentJobJson, jobObject)
    # Finished writing the file to disk indicating job progress/completion
    return


# When a job has finished, send the result to the Davra server        
def reportJobStatus():            
    global flagIsJobRunning
    jobObject = None
    with open(currentJobJson) as data_file:
        jobObject = json.load(data_file) 
    deviceJobObject = jobObject['devices'][0]
    headers = comDavra.getHeadersForRequests()
    apiEndPoint = comDavra.conf['server'] + '/api/v1/jobs/' + jobObject['UUID'] + '/' + deviceJobObject['UUID']
    r = comDavra.httpPut(apiEndPoint, deviceJobObject)
    if (r.status_code == 200):
        comDavra.logEvent('job_finished', 'DEBUG', job_uuid=jobObject['UUID'], **comDavra.logHttpFields(r))
        # The job has completed and been reflected at the server so delete the currentJobJson file
        os.remove(currentJobJson)
    else:
        pass  # The shared HTTP path records the outcome.
    # Report job event to server as an iotdata event
    eventToSend = {
        "UUID": deviceJobObject['UUID'],
        "name": "davra.job.finished",
        "msg_type": "event",
        "value": deviceJobObject,
        "tags": {
            "status": deviceJobObject["status"]
        }
    }
    apiEndPoint = comDavra.conf['server'] + '/api/v1/iotdata'
    r = comDavra.httpPut(apiEndPoint, eventToSend)
    if (r.status_code == 200):
        comDavra.logEvent('job_finished', 'DEBUG', job_uuid=jobObject['UUID'], **comDavra.logHttpFields(r))
    else:
        pass  # The shared HTTP path records the outcome.
    comDavra.provideFreshDirectory(currentJobDir) # Remove the file and dir on disk 
    flagIsJobRunning = False
    return



# Report an event to the Davra server indicating the agent started
def reportAgentStarted():            
    eventToSend = {
        "UUID": comDavra.conf['UUID'],
        "name": "davra.agent.started",
        "msg_type": "event",
        "value": comDavra.conf
    }
    r = comDavra.sendDataToServer(eventToSend)
    if (r.status_code == 200):
        pass  # The shared HTTP path records the outcome.
    # Update the device labels to reflect this agent version
    comDavra.updateDeviceLabelOnServer("davraAgentVersion", comDavra.davraAgentVersion)
    return




###########################   RUN FUNCTIONS

# Run a function which the agent knows what to do, or get the appropriate app to run it
# This function just kicks it off. Another function will check in later to see if it is finished
def runFunction(functionName, functionParameterValues, jobUuid=None):
    global flagIsFunctionRunning
    if not isinstance(functionName, str) or not isinstance(functionParameterValues, dict):
        raise ValueError("invalid_function_message")
    parameters = dict(functionParameterValues)
    functionId = _functionUuid(parameters.setdefault("functionUuid", comDavra.generateUuid()))
    if functionName not in comDavra.conf.get("capabilities", {}):
        comDavra.logProblem('command_rejected', 'command_rejected', 'WARN', function=functionName, function_uuid=functionId, reason='unknown_capability')
        return
    internal = functionName in agentCapabilityFunctions
    fingerprint = _functionFingerprint(functionName, parameters)
    with comDavra.stateLock:
        records = _functionRecords()
        recent = _recentFunctions()
        previous = next((row for _, row in records
                         if row["functionParameterValues"]["functionUuid"] == functionId), None)
        if previous is not None or functionId in recent:
            comDavra.logEvent('command_duplicate', function=functionName, function_uuid=functionId)
            return
        if len(records) >= 256:
            raise RuntimeError("too_many_pending_functions")
        if internal and os.path.exists(currentFunctionJson):
            comDavra.logProblem('command_busy', 'command_rejected', 'WARN', function=functionName, function_uuid=functionId, reason='internal_function_busy')
            return
        functionInfo = {'functionName': functionName, 'functionParameterValues': parameters,
                        'status': 'running', 'startTime': comDavra.getMilliSecondsSinceEpoch(),
                        '_fingerprint': fingerprint}
        if jobUuid is not None:
            functionInfo['_jobUuid'] = jobUuid
        filename = currentFunctionJson if internal else _functionPath(functionId)
        comDavra.writeJsonAtomic(filename, functionInfo)
        flagIsFunctionRunning = True
    comDavra.logEvent('command_received', function=functionName, function_uuid=functionId)
    if internal:
        agentCapabilityFunctions[functionName](parameters)
    else:
        # Agent-private bookkeeping stays local; the Node-RED wire contract is unchanged.
        sendMessageFromAgentToApps({key: value for key, value in functionInfo.items()
                                   if not key.startswith('_')})


# Check if a function is finished running and if it was part of a job
def checkFunctionFinished():
    for filename, _ in _functionRecords():
        with comDavra.stateLock:
            try:
                row = comDavra.readJsonFile(filename)
            except FileNotFoundError:
                continue
            functionId = row['functionParameterValues']['functionUuid']
            if functionId in _finishingFunctions:
                continue
            if row.get('status') not in ('completed', 'failed'):
                if comDavra.getMilliSecondsSinceEpoch() - int(row['startTime']) <= int(comDavra.conf['scriptMaxTime']) * 1000:
                    continue
                row.update({'status': 'failed', 'endTime': comDavra.getMilliSecondsSinceEpoch(),
                            'response': 'function_timeout'})
            attempted = row.get('_eventAttempted', False)
            # Persist the attempt before HTTP. A restart or local write failure must
            # not turn this into a new automatic event resend.
            row['_eventAttempted'] = True
            comDavra.writeJsonAtomic(filename, row)
            _finishingFunctions.add(functionId)
        try:
            status = None
            if not attempted:
                try:
                    status = reportFunctionFinishedAsEventToServer(row)
                except Exception as error:
                    comDavra.logProblem('function_report', 'http_failed', function_uuid=functionId, error_class=type(error).__name__, reason='function_report_exception')
            if row.get('_jobUuid'):
                updateJobWithResult(row['status'], row.get('response', ''), expectedJobUuid=row['_jobUuid'])
            with comDavra.stateLock:
                current = comDavra.readJsonFile(filename)
                if current['functionParameterValues']['functionUuid'] != functionId:
                    raise ValueError('function_identity_mismatch')
                recent = _recentFunctions()
                recent[functionId] = {'functionName': row['functionName'], 'status': row['status'],
                                      'eventHttpStatus': status, 'reportedAt': comDavra.getMilliSecondsSinceEpoch(),
                                      'fingerprint': row.get('_fingerprint')}
                recent = dict(list(recent.items())[-256:])
                comDavra.writeJsonAtomic(os.path.join(currentFunctionDir, '.reported-functions.json'), recent)
                os.unlink(filename)
                _refreshFunctionFlag()
            comDavra.logEvent('command_finished', function_uuid=functionId, result=row['status'], reporting='http_accepted' if status is not None and 200 <= status < 300 else 'not_confirmed')
        finally:
            with comDavra.stateLock:
                _finishingFunctions.discard(functionId)
    checkCurrentJob()


# Report function-finished event to server as an iotdata event
def reportFunctionFinishedAsEventToServer(currentFunctionInfo):
    if(("functionParameterValues" in currentFunctionInfo) == False \
    or ("functionUuid" in currentFunctionInfo["functionParameterValues"]) == False):
        return
    eventToSend = {
        "UUID": comDavra.conf['UUID'],
        "name": "davra.function.finished",
        "msg_type": "event",
        "value": {key: value for key, value in currentFunctionInfo.items() if not key.startswith('_')},
        "tags": {
            "functionUuid": currentFunctionInfo["functionParameterValues"]["functionUuid"]
        }
    }
    #comDavra.log("Sending event to server to indicate function finished: " + str(eventToSend))
    r = comDavra.sendDataToServer(eventToSend)
    comDavra.logEvent('command_report', function_uuid=currentFunctionInfo['functionParameterValues']['functionUuid'], **comDavra.logHttpFields(r))
    return r.status_code
    

# When a function was enacted by a device app rather than the agent, it reports back the
# finished function information via mqtt. This receives that message and updates the
# agent's understanding of the function's progress
def updateFunctionStatusAsReportedByDeviceApp(functionInfo):
    functionId = _functionUuid(functionInfo.get('functionUuid'))
    status = functionInfo.get('status')
    if status not in ('completed', 'failed'):
        raise ValueError('invalid_function_status')
    with comDavra.stateLock:
        records = _functionRecords()
        match = next(((path, row) for path, row in records
                      if row['functionParameterValues']['functionUuid'] == functionId), None)
        if match is None:
            comDavra.logProblem('command_ack', 'command_ack_ignored', 'WARN', function_uuid=functionId, reason='late_or_unknown')
            return
        filename, row = match
        if functionInfo.get('finishedFunctionOnApp') != row['functionName']:
            comDavra.logProblem('command_ack', 'command_ack_ignored', 'WARN', function_uuid=functionId, reason='wrong_function')
            return
        if row.get('status') in ('completed', 'failed') or functionId in _finishingFunctions:
            return
        comDavra.upsertJsonEntries(filename, {'response': functionInfo.get('response', ''),
                                            'status': status,
                                            'endTime': comDavra.getMilliSecondsSinceEpoch()}, functionId)
    checkFunctionFinished()
    return

# Function: Reboot this device        
def agentFunctionReboot(functionParameterValues):
    # Put a file to indicate what is happening and start the reboot process
    with open(currentFunctionDir + '/doingReboot.json', 'w') as outfile:
        json.dump({ 'doingRebootAsPartOfFunction': True }, outfile, indent=4)
    comDavra.logEvent('diagnostic', 'DEBUG', reason='agentFunctionReboot')
    comDavra.runCommandWithTimeout('sudo reboot -h now', comDavra.conf["scriptMaxTime"])


# Check if we are just back after a purposeful reboot as part of a job or function
def checkIfJustBackAfterRebootTask():
    currentFunctionInfo = None
    if(os.path.isfile(currentFunctionJson) == True):
        with open(currentFunctionJson) as data_file:
            currentFunctionInfo = json.load(data_file)
        if(currentFunctionInfo["functionName"] == 'agent-action-rebootDevice'):
            comDavra.logEvent('diagnostic', 'DEBUG', reason='checkIfJustBackAfterRebootTask')
            comDavra.upsertJsonEntry(currentFunctionJson, 'response', str(comDavra.getUptime()))
            comDavra.upsertJsonEntry(currentFunctionJson, 'status', 'completed')
            checkFunctionFinished()             


# Function: Push an Application which has an install.sh onto this device to run as a service
# functionParameterValues should have "Installation File" which should be a tar.gz containing the service file,
# an install.sh 
def agentFunctionPushAppWithInstaller(functionParameterValues):
    comDavra.logEvent('package_received', function_uuid=functionParameterValues.get('functionUuid'))
    if(functionParameterValues["Installation File"]):
        installationFile = functionParameterValues["Installation File"]
        # Download the app tarball
        try:
            tmpPath = '/tmp/' + str(comDavra.getMilliSecondsSinceEpoch())
            comDavra.ensureDirectoryExists(tmpPath)
            comDavra.runCommandWithTimeout('cd ' + tmpPath + ' && /usr/bin/curl -LO ' + installationFile, 300)
            comDavra.runCommandWithTimeout('cd ' + tmpPath + ' && /bin/tar -xvf ./* ', 300)
            comDavra.runCommandWithTimeout('cd ' + tmpPath + ' && chmod 777 ./* ', 300)
            # Ensure the install.sh is unix format
            comDavra.runCommandWithTimeout("cd " + tmpPath + " &&  sed -i $'s/\r$//' install.sh ", 30)
            installedAppPath = comDavra.installationDir + '/apps/' + str(comDavra.getMilliSecondsSinceEpoch())
            comDavra.ensureDirectoryExists(installedAppPath)
            comDavra.runCommandWithTimeout('cd ' + tmpPath + ' && cp -r * ' + installedAppPath, 300)
            installResponse = comDavra.runCommandWithTimeout('cd ' + installedAppPath + ' && bash ./install.sh ', comDavra.conf["scriptMaxTime"])
            comDavra.logEvent('package_finished', function_uuid=functionParameterValues.get('functionUuid'), exit_code=installResponse[0])
            scriptStatus = 'completed'  if (installResponse[0] == 0) else 'failed'
            comDavra.upsertJsonEntry(currentFunctionJson, 'response', str(installResponse[1]))
            comDavra.upsertJsonEntry(currentFunctionJson, 'status', scriptStatus)
        except Exception as e:
            comDavra.logProblem('package_run', 'agent_action_failed', error_class=type(e).__name__, reason='package_failed')
            comDavra.upsertJsonEntry(currentFunctionJson, 'status', 'failed')
            checkFunctionFinished() 
        comDavra.logEvent('diagnostic', 'DEBUG', reason='agentFunctionPushAppWithInstaller')
        checkFunctionFinished()
    else:
        comDavra.logProblem('package_parameters', 'command_rejected', 'WARN', reason='missing_parameters')
    # TODO
    return


# Function: Push an Application onto this device to run as a snap
def agentFunctionPushAppSnap(functionParameterValues):
    comDavra.logEvent('package_received', function_uuid=functionParameterValues.get('functionUuid'))
    if(functionParameterValues["Snap File From Repo"]):
        comDavra.logEvent('diagnostic', 'DEBUG', reason='agentFunctionPushAppSnap');
    # TODO
    return


# Function: Report the device configuration to the server
def agentFunctionReportAgentConfig(functionParameterValues):
    comDavra.logEvent('config_reported', 'DEBUG', function_uuid=functionParameterValues.get('functionUuid'))
    comDavra.reportDeviceConfigurationToServer()
    comDavra.upsertJsonEntry(currentFunctionJson, 'response', comDavra.conf)
    comDavra.upsertJsonEntry(currentFunctionJson, 'status', 'completed')
    checkFunctionFinished()
    return


# Function: Report the device configuration to the server
def agentFunctionUpdateAgentConfig(functionParameterValues):
    comDavra.logEvent('config_reported', 'DEBUG', function_uuid=functionParameterValues.get('functionUuid'))
    comDavra.upsertConfigurationItem(functionParameterValues["key"], functionParameterValues["value"])
    comDavra.reportDeviceConfigurationToServer()
    comDavra.upsertJsonEntry(currentFunctionJson, 'response', comDavra.conf)
    comDavra.upsertJsonEntry(currentFunctionJson, 'status', 'completed')
    checkFunctionFinished()
    return


# Function: Run a bash script
# Take a set of lines and write them to a shell script then execute that script
def agentFunctionRunScriptBash(functionParameterValues):
    if "script" not in functionParameterValues:
        comDavra.upsertJsonEntry(currentFunctionJson, 'response', 'script missing')
        comDavra.upsertJsonEntry(currentFunctionJson, 'status', 'failed')
        comDavra.logProblem('script_parameters', 'command_rejected', reason='missing_script')
        checkFunctionFinished()
        return
    # Put the script into the function dir 
    scriptFile = open(currentFunctionDir + "/script.sh", "a")
    scriptFile.write(str(functionParameterValues["script"]))
    scriptFile.close()
    comDavra.logEvent('script_received', function_uuid=functionParameterValues.get('functionUuid'))
    os.system("chmod 777 " + currentFunctionDir + "/script.sh")
    time.sleep(0.5) # Time for file flush to disk
    # Run the script with -x flag so it prints each command before ruuning it. 
    # This allows the UI to show it formatted better for user on jobs page
    scriptResponse = comDavra.runCommandWithTimeout('cd ' + currentFunctionDir + \
    ' && sudo bash -x ' + currentFunctionDir + "/script.sh", comDavra.conf["scriptMaxTime"])
    # scriptResponse is a tuple of (exitStatusCode, stdout) . For exitStatusCode: 0 = success, 1 = failed )
    comDavra.logEvent('script_finished', function_uuid=functionParameterValues.get('functionUuid'), exit_code=scriptResponse[0])
    scriptStatus = 'completed'  if (scriptResponse[0] == 0) else 'failed'
    comDavra.upsertJsonEntry(currentFunctionJson, 'response', str(scriptResponse[1]))
    comDavra.upsertJsonEntry(currentFunctionJson, 'status', scriptStatus)
    checkFunctionFinished()



###########################   REPORT KNOWN AGENT CAPABILITIES


# Set which actions this agent can do and inform the server of these capabilities
agentCapabilityFunctions = {} # Keep a pointer to the functions which enact each of capabilities
def registerAgentCapabilities(functionName, functionDetails, functionToCallToEnactCapability):
    global agentCapabilityFunctions
    agentCapabilityFunctions[functionName] = functionToCallToEnactCapability
    # Inform the server that this device has these capabilities
    comDavra.registerDeviceCapability(functionName, functionDetails)


# Register the functions defined above as those which the agent can do
# This will then be sent to the server informing the server of agent capabilities
# This makes its way into the value at /api/v1/devices/<deviceUuid> in "capabilities"
# Device Applications may also register their own capabilities separately
#
def registerAllAgentCapabilities():
    registerAgentCapabilities('agent-action-pushAppWithInstaller', { \
        "functionParameters": { "Installation File": "file" }, \
        "functionLabel": "Push Device App (with installer)", \
        "functionDescription": "To run a device Application alongside the Device Agent on a device. Supply a tar.gz file containing an install.sh script to install it." \
    }, agentFunctionPushAppWithInstaller)
    # registerAgentCapabilities('agent-action-pushAppDocker', { \
    #     "functionParameters": { "Application Name": "string", "File URL": "file" }, \
    #     "functionLabel": "Push Device App (Docker)", \
    #     "functionDescription": "To run a device Application alongside the Device Agent on a device. Supply a tar.gz file with the serviceName.service file" \
    # }, agentFunctionPushAppDocker)
    # registerAgentCapabilities('agent-action-pushAppSnap', { \
    #     "functionParameters": { "Application Name": "string", "Snap File From Repo": "file" }, \
    #     "functionLabel": "Push Device App (Snap)", \
    #     "functionDescription": "To run a device Application alongside the Device Agent on a device. Supply a location of the snap" \
    # }, agentFunctionPushAppSnap)
    registerAgentCapabilities('agent-action-rebootDevice', { \
        "functionParameters": {}, \
        "functionLabel": "Reboot Device", \
        "functionDescription": "Reboot the device immediately" \
    }, agentFunctionReboot)
    registerAgentCapabilities('agent-action-reportAgentConfig', { \
        "functionParameters": {}, \
        "functionLabel": "Report Agent Config to Server", \
        "functionDescription": "Send the agent configuration from agent to server" \
    }, agentFunctionReportAgentConfig)
    registerAgentCapabilities('agent-action-updateAgentConfig', { \
        "functionParameters": { "key": "string", "value": "string" }, \
        "functionLabel": "Update Config on Device Agent", \
        "functionDescription": "Upsert a configuration item on the device" \
    }, agentFunctionUpdateAgentConfig)
    registerAgentCapabilities('agent-action-runScriptBash', { \
        "functionParameters": { "script": "textarea" }, \
        "functionLabel": "Run bash script on Device", \
        "functionDescription": "Run a bash script once on the device, launched by the Device Agent" \
    }, agentFunctionRunScriptBash)

###########################   MQTT Broker running on device

# The deployed server path closes an idle connection at about 60 seconds,
# before Paho's default 60-second PING can reliably reach it. Keep the
# connection alive below that boundary; this does not replay commands.
SERVER_MQTT_KEEPALIVE_SECONDS = 30
_mqttLock = threading.RLock()
_mqttStates = {role: {'client': None, 'thread': None, 'connected': False,
                      'subscribed': False, 'stopping': False, 'starts': 0,
                      'nextStart': 0, 'delay': 2, 'disconnections': 0,
                      'lastProblem': None, 'lastProblemAt': 0,
                      'subscribeMid': None, 'disconnectedAt': None}
               for role in ('device', 'server')}


class MqttCleanupError(RuntimeError):
    pass


def _mqttProblem(role, reason, error=None, code=None):
    comDavra.logProblem('mqtt_' + role, 'mqtt_problem', 'WARN', role=role,
                        reason=reason, error_class=type(error).__name__ if error is not None else None,
                        reason_code=code, version=comDavra.davraAgentVersion)



def _mqttRole(client):
    with _mqttLock:
        return next((role for role, state in _mqttStates.items()
                     if state['client'] is client), None)


def _mqttConnected(client, resultCode, topic):
    role = _mqttRole(client)
    if role is None:
        return
    with _mqttLock:
        state = _mqttStates[role]
        state.update({'connected': False, 'subscribed': False, 'subscribeMid': None})
    if int(resultCode) != 0:
        _mqttProblem(role, 'connection_rejected', code=int(resultCode))
        return
    try:
        result, mid = client.subscribe(topic)
    except Exception as error:
        _mqttProblem(role, 'subscribe_failed', error)
        client.disconnect()
        return
    if int(result) != 0:
        _mqttProblem(role, 'subscribe_failed', code=int(result))
        client.disconnect()
        return
    with _mqttLock:
        state = _mqttStates[role]
        state['connected'] = True
        state['subscribeMid'] = mid
        state['delay'] = 2
        state['lastProblem'] = None
    # This callback runs on Paho's receiver. A remote log HTTP call here
    # would delay SUBACK/PING/command handling; local logs and the existing
    # heartbeat retain observability without adding another sender.
    comDavra.logEvent('mqtt_connected', 'DEBUG', role=role, version=comDavra.davraAgentVersion)


def mqttOnSubscribe(client, userdata, mid, grantedQos, *unused):
    role = _mqttRole(client)
    if role is None:
        return
    with _mqttLock:
        if _mqttStates[role]['subscribeMid'] != mid:
            return
        alreadyReady = _mqttStates[role]['subscribed']
        accepted = bool(grantedQos) and all(int(value) < 128 for value in grantedQos)
        _mqttStates[role]['subscribed'] = accepted
    if not accepted:
        _mqttProblem(role, 'subscription_rejected')
        client.disconnect()
    elif not alreadyReady:
        with _mqttLock:
            downAt = _mqttStates[role]['disconnectedAt']
            _mqttStates[role]['disconnectedAt'] = None
        comDavra.logEvent('mqtt_ready', role=role, version=comDavra.davraAgentVersion,
                          loop_starts=_mqttStates[role]['starts'],
                          keepalive_s=SERVER_MQTT_KEEPALIVE_SECONDS if role == 'server' else 60,
                          reconnect_ms=round((time.monotonic()-downAt)*1000) if downAt is not None else 0)
        comDavra.logRecovered('mqtt_' + role)
    comDavra.pollLogging(mqttHealth())


def mqttOnDisconnect(client, userdata, resultCode, *unused):
    role = _mqttRole(client)
    if role is None:
        return
    with _mqttLock:
        state = _mqttStates[role]
        state['connected'] = False
        state['subscribed'] = False
        state['subscribeMid'] = None
        stopping = state['stopping']
        if not stopping:
            state['disconnections'] += 1
            if state['disconnectedAt'] is None:
                state['disconnectedAt'] = time.monotonic()
    if not stopping:
        _mqttProblem(role, 'disconnected', code=int(resultCode))
    comDavra.pollLogging(mqttHealth())


def _mqttMessage(role, msg, processor, allowAnnouncement=False):
    try:
        payload = msg.payload.decode('utf8')
        if allowAnnouncement and payload == 'davra.announcement:check-for-jobs':
            value = {'stringMsg': payload}
        else:
            value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError('mqtt_object_required')
        processor(value)
    except Exception as error:
        # One bad input or application callback must not kill Paho's receiver.
        _mqttProblem(role, 'message_failed', error)


def mqttHealth():
    with _mqttLock:
        return {role: {'connected': state['connected'], 'subscribed': state['subscribed'],
                       'loopAlive': bool(state['thread'] and state['thread'].is_alive()),
                       'loopStarts': state['starts'], 'disconnections': state['disconnections']}
                for role, state in _mqttStates.items()}


def _newMqttClient(role):
    client = mqtt.Client()
    # Track ownership before configuration or connect_async can fail. This also
    # covers Paho's socket pair allocated by the constructor.
    with _mqttLock:
        _mqttStates[role].update({'client': client, 'stopping': False})
    return client


def _closeMqttClient(role):
    with _mqttLock:
        state = _mqttStates[role]
        client, worker = state['client'], state['thread']
        state['stopping'] = True
    if client is None:
        return
    try:
        client.disconnect()
    except Exception as error:
        _mqttProblem(role, 'disconnect_failed', error)
    if worker is not None and worker.is_alive():
        if worker is threading.current_thread():
            raise MqttCleanupError('mqtt_cleanup_on_receiver_thread')
        worker.join(timeout=45)
        if worker.is_alive():
            # Do not create a second live receiver. Let systemd restart the
            # process instead; old commands remain recorded, never replayed.
            raise MqttCleanupError('mqtt_worker_did_not_stop')
    try:
        client.loop_stop()
    except Exception as error:
        _mqttProblem(role, 'loop_stop_failed', error)
    for socketName in ('_sock', '_sockpairR', '_sockpairW'):
        sock = getattr(client, socketName, None)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
            setattr(client, socketName, None)
    with _mqttLock:
        state.update({'client': None, 'thread': None, 'connected': False,
                      'subscribed': False, 'subscribeMid': None})


def manageMqttClients():
    global clientOfDevice, clientOfServer
    for role, factory in (('device', mqttConnectToDevice), ('server', mqttConnectToServer)):
        state = _mqttStates[role]
        worker = state['thread']
        if worker is not None and worker.is_alive():
            continue  # Paho owns ordinary reconnect while its loop is alive.
        if state['client'] is not None:
            _mqttProblem(role, 'loop_stopped')
            _closeMqttClient(role)
        if _stopRequested.is_set() or time.monotonic() < state['nextStart']:
            continue
        try:
            client = factory()
            if client is None:
                raise ValueError('mqtt_configuration_unavailable')
            with _mqttLock:
                state.update({'client': client, 'stopping': False})
            client.on_disconnect = mqttOnDisconnect
            client.on_subscribe = mqttOnSubscribe
            client.reconnect_delay_set(min_delay=2, max_delay=60)
            result = client.loop_start()
            if result is not None and int(result) != 0:
                raise RuntimeError('mqtt_loop_start_failed')
            with _mqttLock:
                state['thread'] = getattr(client, '_thread', None)
                state['starts'] += 1
                state['nextStart'] = time.monotonic() + state['delay']
            comDavra.logEvent('mqtt_loop_started', role=role, version=comDavra.davraAgentVersion, loop_starts=state['starts'])
        except Exception as error:
            _mqttProblem(role, 'start_failed', error)
            if state['client'] is not None:
                _closeMqttClient(role)
            state['nextStart'] = time.monotonic() + state['delay']
            state['delay'] = min(60, state['delay'] * 2)


def closeMqttClients():
    for role in ('device', 'server'):
        _closeMqttClient(role)

# The callback for when the client receives a CONNACK response from the broker on the device.
def mqttOnConnectDevice(client, userdata, flags, resultCode):
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    _mqttConnected(client, resultCode, "/agent")
    return


# The callback for when a message is received from the mqtt broker on the device.
def mqttOnMessageDevice(client, userdata, msg):
    _mqttMessage('device', msg, processMessageFromAppToAgent)
    

# Setup the MQTT client talking to the broker on the device    
clientOfDevice = None
def mqttConnectToDevice():
    global clientOfDevice
    if not comDavra.conf.get('mqttBrokerAgentHost'):
        return None
    clientOfDevice = _newMqttClient('device')
    clientOfDevice.on_connect = mqttOnConnectDevice
    clientOfDevice.on_message = mqttOnMessageDevice
    clientOfDevice.username_pw_set(username = comDavra.conf["UUID"], password = comDavra.conf["apiToken"])
    clientOfDevice.connect_async(comDavra.conf['mqttBrokerAgentHost'])
    return clientOfDevice




###########################   Process Messages from Device Application to Device Agent

# These messages may arrive by mqtt from app to agent or api calls or flat file comms
# msg should be a json object
def processMessageFromAppToAgent(msg):
    # Ignore any messages this agent published
    if("fromAgent" in msg):
        return
    if("registerCapability" in msg):
        capabilityName = msg["registerCapability"]
        capabilityDetails = msg["capabilityDetails"] if "capabilityDetails" in msg else {}
        comDavra.registerDeviceCapability(capabilityName, capabilityDetails)
    if("runFunctionOnAgent" in msg):
        functionName = msg["runFunctionOnAgent"]
        functionParameterValues = msg["functionParameterValues"] if "functionParameterValues" in msg else {}
        runFunction(functionName, functionParameterValues)
    if("connectToAgent" in msg):
        applicationName = msg["connectToAgent"]
        time.sleep(0.1)
        sendHeartbeatToDeviceApps()
    if("retrieveConfigFromAgent" in msg):
        sendMessageFromAgentToApps({"agentConfig": comDavra.conf})
    if("finishedFunctionOnApp" in msg):
        functionName = msg["finishedFunctionOnApp"]
        updateFunctionStatusAsReportedByDeviceApp(msg)
    if("sendIotData" in msg):
        sendIotDataToServer(msg)


# Send a message onto the mqtt topic which the Device Apps are lstening to
# Usually topic /agent on localhost
# msg should be valid json
def sendMessageFromAgentToApps(msg):
    msg['fromAgent'] = comDavra.davraAgentVersion
    if clientOfDevice is None:
        raise RuntimeError('mqtt_device_client_unavailable')
    result = clientOfDevice.publish('/agent', json.dumps(msg))
    if int(result.rc) != 0:
        comDavra.logProblem('mqtt_publish_device', 'mqtt_problem', role='device', reason='publish_failed', reason_code=int(result.rc))


# Send a regular message to apps to inform them the agent is still available
def sendHeartbeatToDeviceApps():
    sendMessageFromAgentToApps({"agentHeartbeat": comDavra.getMilliSecondsSinceEpoch()})


# Send metrics and events to the platform server
def sendIotDataToServer(msgFromMqtt):
    dataFromAgent = json.loads(msgFromMqtt["sendIotData"])
    if (type (dataFromAgent) == type ({})):
        dataFromAgent = [dataFromAgent]
    dataForServer = []
    invalidCount = 0
    for metric in dataFromAgent:
        if (("UUID" in metric) == False):
            metric["UUID"] = comDavra.conf["UUID"]
        if ("timestamp" not in metric):
            metric["timestamp"] = comDavra.getMilliSecondsSinceEpoch()
        if ("name" in metric and "value" in metric and "msg_type" in metric):
            dataForServer.append(metric)
        else:
            invalidCount += 1
            comDavra.countDiagnostic('invalid_datapoints')
    if invalidCount:
        comDavra.logProblem('telemetry_validation', 'metric_invalid', count=invalidCount)
    else:
        comDavra.logRecovered('telemetry_validation')
    if dataForServer:
        response = comDavra.sendDataToServer(dataForServer)
        failed = bool(getattr(response, 'transport_error', None)) or not 200 <= response.status_code < 300
        comDavra.countDiagnostic('telemetry_batches_failed' if failed else 'telemetry_batches_ok')
        comDavra.countDiagnostic('telemetry_datapoints_failed' if failed else 'telemetry_datapoints_ok', len(dataForServer))
    
    
        
        


###########################   MQTT Broker running on the Davra server (probably mqtt.davra.com)

# A device can subscribe to a topic for itself on "devices/<deviceUuid>"
# the Davra server will communicate over this topic to speak to the agent and vice-versa
# Usually the agent makes regular API calls to /api/v1/jobs to determine if there is a job to run and 
# download the job. When a new job is created on the server, a "new-job-available" mesasge will be 
# sent over mqtt to indicate this

# The callback for when the client (this agent) receives a CONNACK response from the broker
def mqttOnConnectServer(client, userdata, flags, resultCode):
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    _mqttConnected(client, resultCode, "devices/" + comDavra.conf["UUID"])
    return


# The callback for when a message is received from the broker on platform server.
def mqttOnMessageServer(client, userdata, msg):
    _mqttMessage('server', msg, processMessageFromServerToAgent, allowAnnouncement=True)

    
# Setup the MQTT client talking to the broker on the Davra server    
clientOfServer = None
def mqttConnectToServer():
    global clientOfServer
    if("mqttBrokerServerHost" in comDavra.conf and len(comDavra.conf["mqttBrokerServerHost"]) > 3) \
    and "apiToken" in comDavra.conf:
        clientOfServer = _newMqttClient('server')
        clientOfServer.on_connect = mqttOnConnectServer
        clientOfServer.on_message = mqttOnMessageServer
        apiTokenForDevice = comDavra.conf["apiToken"]
        clientOfServer.username_pw_set(username = comDavra.conf["UUID"], password = apiTokenForDevice)
        
        # Configure TLS/SSL if enabled in configuration
        useTls = comDavra.conf.get("mqttBrokerServerUseTLS", False)
        mqttPort = comDavra.conf.get("mqttBrokerServerPort", 8883 if useTls else 1883)
        
        if useTls:
            try:
                # Get TLS configuration from config file
                ca_certs = comDavra.conf.get("mqttBrokerServerCaCert", None)
                certfile = comDavra.conf.get("mqttBrokerServerClientCert", None)
                keyfile = comDavra.conf.get("mqttBrokerServerClientKey", None)
                # Default to auto-negotiation (PROTOCOL_TLS) for best compatibility
                tls_version = comDavra.conf.get("mqttBrokerServerTlsVersion", None)
                cert_required = comDavra.conf.get("mqttBrokerServerCertRequired", True)
                
                # Map TLS version string to ssl constant
                # If no version specified, use PROTOCOL_TLS which auto-negotiates the best version
                tls_version_map = {
                    "TLSv1": ssl.PROTOCOL_TLSv1,
                    "TLSv1.1": ssl.PROTOCOL_TLSv1_1,
                    "TLSv1.2": ssl.PROTOCOL_TLSv1_2,
                    "TLS": ssl.PROTOCOL_TLS  # Auto-negotiate best version
                }
                # Add TLSv1.3 if available (Python 3.7+)
                if hasattr(ssl, 'PROTOCOL_TLSv1_3'):
                    tls_version_map["TLSv1.3"] = ssl.PROTOCOL_TLSv1_3
                
                # Use PROTOCOL_TLS as default for auto-negotiation (best practice)
                if tls_version:
                    tls_protocol = tls_version_map.get(tls_version, ssl.PROTOCOL_TLS)
                else:
                    tls_protocol = ssl.PROTOCOL_TLS
                
                # Configure TLS settings
                clientOfServer.tls_set(
                    ca_certs=ca_certs if ca_certs else None,
                    certfile=certfile if certfile else None,
                    keyfile=keyfile if keyfile else None,
                    cert_reqs=ssl.CERT_REQUIRED if cert_required else ssl.CERT_NONE,
                    tls_version=tls_protocol
                )
                
                # Enable SNI (Server Name Indication) - required by many modern MQTT brokers
                # This is automatically handled by paho-mqtt when we call connect() with the hostname
                
                # Option to disable certificate hostname verification (not recommended for production)
                if not comDavra.conf.get("mqttBrokerServerVerifyHostname", True):
                    comDavra.logProblem('mqtt_server_config', 'mqtt_problem', 'WARN', role='server', reason='hostname_verification_disabled')
                    clientOfServer.tls_insecure_set(True)
                    
            except Exception as e:
                comDavra.logProblem('mqtt_server_config', 'mqtt_problem', role='server', reason='tls_config_failed', error_class=type(e).__name__)
                return
        
        try:
            clientOfServer.connect_async(comDavra.conf["mqttBrokerServerHost"], port=mqttPort,
                                        keepalive=SERVER_MQTT_KEEPALIVE_SECONDS)
            return clientOfServer
        except ssl.SSLError as e:
            comDavra.logProblem('mqtt_server_config', 'mqtt_problem', role='server', reason='connect_setup_failed', error_class=type(e).__name__)
        except Exception as e:
            comDavra.logProblem('mqtt_server_config', 'mqtt_problem', role='server', reason='connect_setup_failed', error_class=type(e).__name__)
    else:
        comDavra.logProblem('mqtt_server_config', 'mqtt_problem', role='server', reason='broker_not_configured')



###########################   Process Messages from Davra Server to this Device Agent

# These messages may arrive by mqtt from server to agent 
# msg should be a json object
def processMessageFromServerToAgent(msg):
    if("stringMsg" in msg and msg["stringMsg"] == "davra.announcement:check-for-jobs"):
        checkForPendingJob()
    if("davra-announcement" in msg and msg["davra-announcement"] == "check-for-jobs"):
        checkForPendingJob()
    if("davra-function" in msg):
        funcParamsToRun = {}
        if("functionParameterValues" in msg):
            funcParamsToRun = msg["functionParameterValues"]
        runFunction(msg["davra-function"], funcParamsToRun)
        



###########################   MAIN LOOP

def _mainAction(name, action):
    try:
        action()
        comDavra.logRecovered('agent_' + name)
    except MqttCleanupError:
        raise
    except Exception as error:
        comDavra.logProblem('agent_' + name, 'agent_action_failed', reason=name,
                            error_class=type(error).__name__)



def main(debugSeconds=0):
    if 'server' not in comDavra.conf or 'UUID' not in comDavra.conf:
        comDavra.logEvent('agent_config_invalid', 'ERROR', reason='missing_identity_or_server')
        comDavra.closeLogging(timeout=3)
        return 1
    interval = int(comDavra.conf['heartbeatInterval'])
    if interval <= 0:
        raise ValueError('invalid_heartbeat_interval')
    if debugSeconds:
        comDavra.enableDebugLogging(debugSeconds)
    comDavra.logEvent('agent_started', version=comDavra.davraAgentVersion, forward=True)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda number, frame: _stopRequested.set())
    try:
        manageMqttClients()
        _mainAction('report_started', reportAgentStarted)
        _mainAction('reboot_result', checkIfJustBackAfterRebootTask)
        _mainAction('register_capabilities', registerAllAgentCapabilities)
        _refreshFunctionFlag()
        countMainLoop = 0
        while not _stopRequested.is_set():
            _mainAction('mqtt_supervision', manageMqttClients)
            comDavra.pollLogging(mqttHealth())
            if countMainLoop % interval == 0:
                _mainAction('local_heartbeat', sendHeartbeatToDeviceApps)
                _mainAction('server_heartbeat', sendHeartbeatMetricsToServer)
                _mainAction('pending_job_check', checkForPendingJob)
            if flagIsFunctionRunning or countMainLoop % 60 == 0:
                _mainAction('function_completion', checkFunctionFinished)
            if flagIsJobRunning or countMainLoop % 60 == 0:
                _mainAction('job_completion', checkCurrentJob)
            if countMainLoop % (interval * 10) == 0:
                _mainAction('report_capabilities', comDavra.reportDeviceCapabilities)
            countMainLoop += 1
            _stopRequested.wait(1)
    except KeyboardInterrupt:
        _stopRequested.set()
    finally:
        _stopRequested.set()
        try:
            closeMqttClients()
        finally:
            comDavra.logEvent('agent_stopped', version=comDavra.davraAgentVersion)
            closed = comDavra.closeLogging(timeout=5)
            if not closed['clean']:
                print('<3>ERROR: Loggning hann inte avslutas; processens återstående trådar stängs', flush=True)
    return 0 if closed['clean'] else 1


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug-seconds', type=int, default=0, choices=range(0, 601), metavar='0..600')
    sys.exit(main(parser.parse_args().debug_seconds))
