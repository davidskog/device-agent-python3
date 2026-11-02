# Utility functions for other programs
#
import subprocess
import os, string
import time, requests, os.path
from requests.auth import HTTPBasicAuth
import json 
from pprint import pprint
import sys
import uuid
import threading
import tempfile
from urllib.parse import urlsplit
from datetime import datetime

# Update this when anything changes in the agent
davraAgentVersion = "TLS_1_0"

# Short, process-local critical sections for Agent state and local log files.
# Never hold either lock while doing HTTP, MQTT or running an application.
stateLock = threading.RLock()
_logLock = threading.RLock()

installationDir = "/usr/bin/davra"
# Config file for the agent running on this device
agentConfigFile = installationDir + "/config.json"
# Where logs are saved by default
logDir = "/var/log"
# Flags to indicate cache entries
flagNewCapabilityReadyToReport = False


# Load configuration if it exists
conf = {}
def loadConfiguration():
    global conf
    try:
        if(os.path.isfile(agentConfigFile) is True):
            with open(agentConfigFile) as data_file:
                conf = json.load(data_file)
    except:
        print(('ERROR: Cannot read config file ' + agentConfigFile))
loadConfiguration()


def getConfiguration():
    return conf


# Update (or insert) a configuration item with a value
def upsertConfigurationItem(itemKey, itemValue):
    global conf
    loadConfiguration()
    if(os.path.isfile(agentConfigFile) is True):
        # If this a new key or an alteration of the current config
        if((itemKey in conf) == False or conf[itemKey] != itemValue):
            # Update the item
            conf[itemKey] = itemValue
            # Write the full config file to disk
            with open(agentConfigFile, 'w') as outfile:
                json.dump(conf, outfile, indent=4)
            time.sleep(0.2)
            # Update local cached version of configs
            loadConfiguration()
            # Report the new configuration to the server as an event
            reportDeviceConfigurationToServer()
    return


# Send all of the configuration file to the server as an event
def reportDeviceConfigurationToServer():
    dataToSend = { 
        "UUID": conf['UUID'],
        "name": "davra.agent.configured",
        "value": {
            'deviceConfig': conf
        },
        "msg_type": "event"
    }
    # Inform user of the overall data being sent for a single metric
    logInfo('Sending configuration file to server: ' + conf['server'])
    logInfo(json.dumps(dataToSend, indent=4))
    sendDataToServer(dataToSend)
    log("reportDeviceConfigurationToServer finished.")
    return

# Send a log message to the server
def logToServer(severity, message):
    # Do not send log to server if severity not in the required set
    if severity not in ("ERROR", "WARN", "INFO"):
        return
    dataToSend = { 
        "UUID": conf['UUID'],
        "name": "davra.log",
        "value": {
            "severity": severity,
            "message": message
        }
    }
    sendLogToServer(dataToSend)
    return

# Send various severities of log messages with easy function names
def logDebug(log_msg):
    log(log_msg, "DEBUG")

def logInfo(log_msg):
    log(log_msg, "INFO")

def logWarning(log_msg):
    log(log_msg, "WARN")

def logError(log_msg):
    # Python 3 strings are already decoded, no need to decode
    if isinstance(log_msg, bytes):
        log_msg = log_msg.decode('utf8', errors='replace')
    log(log_msg, "ERROR")

# Log a message to disk and console
def log(log_msg, severity = "DEBUG", forward = True):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = str(log_msg)
    try:
        print(log_time + ": " + severity + ": " + log_msg, flush=True)
    except (OSError, ValueError):
        pass
    try:
        with _logLock:
            filename = os.path.join(logDir, "davra_agent.log")
            try:
                file_size = os.path.getsize(filename)
            except FileNotFoundError:
                file_size = 0
            if file_size > 10000000:
                os.replace(filename, filename + ".old")
            with open(filename, "a") as logfile:
                logfile.write(log_time + ": " + severity + ": " + log_msg + "\n")
    except (OSError, ValueError) as error:
        try:
            print(log_time + ": ERROR: local_log_failed error=" + type(error).__name__, flush=True)
        except (OSError, ValueError):
            pass
    if forward:
        try:
            logToServer(severity, log_msg)
        except Exception as error:
            log("remote_log_failed error=" + type(error).__name__, "ERROR", forward=False)


def getHeadersForRequests():
    headersTmp = {'Accept' : 'application/json', \
        'Content-Type' : 'application/json', \
        'Authorization': 'Bearer ' + conf['apiToken']}
    return headersTmp


def createMetricOnServer(metricName, metricUnits, metricDescription):
    #contents = '[{ "name": "' + metricName + '", '\
    #    + '"label": "' + metricName + '", '\
    #    + '"description": "' + metricDescription + '", '\
    #    + '"semantics": "metric" }]'
    contents = [ { "name": metricName, "label": metricName, "description": metricDescription, "semantics": "metric" } ]
    headers = getHeadersForRequests()
    r = httpPost(conf['server'] + '/api/v1/iotdata/meta-data', contents)
    if(r.status_code == 200):
        log("Metric created on server: " + metricName)
    else:
        log("Failed to create metric on server: " + metricName + ' : ' + str(r.status_code))
    return


# When sending iot data, this is a shorter function to use then regular put
def sendDataToServer(dataToSend):
    responseFromServer = httpPut(conf['server'] + '/api/v1/iotdata', dataToSend)
    return(responseFromServer)

def sendLogToServer(dataToSend):
    responseFromServer = httpPut(conf['server'] + '/api/v1/logs', dataToSend)
    return(responseFromServer)


# For when a http request fails, use this to return a similar object
class emptyRequestsObject(object):
    status_code = 500
    content = ""


def _requestReferences(data):
    """A bounded diagnostic projection, never payloads or authentication."""
    rows = data if isinstance(data, list) else [data]
    result = []
    for row in rows[:8]:
        if not isinstance(row, dict):
            continue
        item = {}
        for key in ("name", "msg_type", "UUID"):
            value = row.get(key)
            if isinstance(value, str):
                item[key] = value[:128]
        tags = row.get("tags")
        if isinstance(tags, dict) and isinstance(tags.get("functionUuid"), str):
            item["functionUuid"] = tags["functionUuid"][:64]
        if item:
            result.append(item)
    return result


def _httpRequest(method, destination, data=None):
    """Exactly one HTTP call. Errors are local-only to avoid log recursion."""
    started = time.monotonic()
    endpoint = 'invalid_destination'
    try:
        endpoint = urlsplit(destination).path
        headers = getHeadersForRequests()
        if method == "GET":
            response = requests.get(destination, headers=headers, timeout=20)
        else:
            function = requests.put if method == "PUT" else requests.post
            response = function(destination, data=json.dumps(data), headers=headers, timeout=20)
        if not 200 <= response.status_code < 300:
            log("http_failed method=" + method + " endpoint=" + endpoint +
                " status=" + str(response.status_code) +
                " elapsed_ms=" + str(int((time.monotonic() - started) * 1000)) +
                " references=" + json.dumps(_requestReferences(data)), "ERROR", forward=False)
        return response
    except Exception as error:
        log("http_failed method=" + method + " endpoint=" + endpoint +
            " status=transport_error error=" + type(error).__name__ +
            " elapsed_ms=" + str(int((time.monotonic() - started) * 1000)) +
            " references=" + json.dumps(_requestReferences(data)), "ERROR", forward=False)
        result = emptyRequestsObject()
        result.transport_error = type(error).__name__
        return result

# Make a http request of type PUT
# Supply the destination API endpoint as string and the dataToSend as JSON object
def httpPut(destination, dataToSend):
    return _httpRequest("PUT", destination, dataToSend)



# Make a http request of type POST
# Supply the destination API endpoint as string and the dataToSend as JSON object
def httpPost(destination, dataToSend):
    return _httpRequest("POST", destination, dataToSend)


# Make a http request of type GET
# Supply the destination API endpoint as string
def httpGet(destination):
    return _httpRequest("GET", destination)


# Send the device capabilities from config file up to server at /api/v1/devices
def reportDeviceCapabilities():
    headers = getHeadersForRequests()
    dataToSend = { "capabilities": conf["capabilities"] }
    log('Reporting device capabilities to server ' + str(dataToSend))
    r = httpPut(conf['server'] + '/api/v1/devices/' + conf["UUID"], dataToSend)
    if (r.status_code == 200):
        log('Reported device capabilities to server ' + str(r.content))
        return(r.status_code)
    else:
        log("Issue while reporting capabilities to server. " + str(r.status_code))
        log(r.content)
        return(r.status_code) 


# Update (or insert) a configuration item with a capability for this device
def registerDeviceCapability(itemKey, itemValue):
    logInfo('registerDeviceCapability ' + str(itemKey) + ': ' + str(itemValue));
    listCapabilities = conf["capabilities"] if "capabilities" in conf else {}
    # If the capability was already known (and in the config.info)
    if((itemKey in listCapabilities) == False or listCapabilities[itemKey] != itemValue):
        log('registerDeviceCapability : this is a new capability')
        listCapabilities[itemKey] = itemValue
        upsertConfigurationItem("capabilities", listCapabilities)
        reportDeviceCapabilities()
    return;


# Delete a configuration item of a capability for this device
def unregisterDeviceCapability(itemKey):
    logInfo('unregisterDeviceCapability ' + str(itemKey));
    listCapabilities = conf["capabilities"] 
    # If the capability was already known (and in the config.info)
    if((itemKey in listCapabilities) == True):
        listCapabilities.pop(itemKey)
        upsertConfigurationItem("capabilities", listCapabilities)
        reportDeviceCapabilities()
    return;


def getLanIpAddress():
    # Returns the current LAN IP address.
    arg = "ip route list"
    p = subprocess.Popen(arg,shell=True,text=True,stdout=subprocess.PIPE)
    data = p.communicate()
    logInfo(data)
    split_data = data[0].split()
    ipaddr = split_data[split_data.index("src")+1]
    return ipaddr




###########################   MQTT


def checkIsAgentMqttBrokerInstalled():
    checkProcess = runCommandWithTimeout(' systemctl status mosquitto ', 10)[1]
    if(len(checkProcess) > 5):
        return True
    else:
        return False



###########################   RUN OS COMMANDS


# Runs command on operating system 
def runNativeCommand(arrayCommandLineParts = []):
    log("Running native command " + str(arrayCommandLineParts))
    try:
        s = subprocess.run(arrayCommandLineParts, capture_output=True, text=True).stdout
        log('Response after cmd:' + str(s))
        return s
    except Exception as e:
        log('Failed to run command:' + str(s) + " : " + str(e))
        return 'unknown'


def runNativeCommandLine(strCommandLine):
    return runNativeCommand(strCommandLine.split())


# Execute command line and return the exit code and stdout
# scriptResponse is a tuple of (exitStatusCode, stdout)
def runCommandWithTimeout(command, timeout):
    timeout = int(timeout)
    log("Running command with timeout " + command + " timeout:" + str(timeout))
    p = subprocess.Popen(command, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while timeout > 0:
        exitStatusCode = p.poll()
        if exitStatusCode is not None:
            # Command finished running (exitStatusCode is 0 when ok)
            log("command finished. Exit code: " + str(exitStatusCode))
            return (exitStatusCode, p.communicate()[0])
        time.sleep(0.1)
        timeout -= 0.1
    else:
        # Script has timed out so kill it
        try:
            p.kill()
        except OSError as e:
            if e.errno != 3:
                raise
    return (-1, None)


# Returns operating system detail
def getOperatingSystem():
    retVal = runNativeCommandLine("grep PRETTY_NAME /etc/os-release")
    if(retVal != None and len(retVal) > 1 and '=' in retVal):
        return retVal.split('=')[1]
    else:
        return runNativeCommandLine("uname -a")



def getRam():
    #Returns a tuple (total ram, available ram) in megabytes.
    try:
        s = subprocess.run(["free", "-m"], capture_output=True, text=True).stdout
        lines = s.split("\n")
        return ( int(lines[1].split()[1]), int(lines[2].split()[3]) )
    except:
        return (0, 0)

def getProcessCount():
    #Returns the number of processes.
    try:
        s = subprocess.run(["ps","-e"], capture_output=True, text=True).stdout
        return len(s.split("\n"))
    except:
        return 0

def getUptime():
    #Returns a tuple (uptime, 1 min load average).
    try:
        s = subprocess.run(["uptime"], capture_output=True, text=True).stdout
        load_split = s.split("load average: ")
        load_one = float(load_split[1].split(",")[0])
        up = load_split[0]
        up_pos = up.rfind(",", 0, len(up)-4)
        up = up[:up_pos].split("up ")[1]
        return (up, load_one)
    except:
        return ("", 0)

def getUptimeProcess():
    # Returns uptime in seconds
    try:
        s = subprocess.run(["cat", "/proc/uptime"], capture_output=True, text=True).stdout
        return int(float(s.split(" ")[0]))
    except:
        return 0


###########################   Utilities


def updateDeviceLabelOnServer(labelKey, labelValue):
    headers = getHeadersForRequests()
    r = httpGet(conf['server'] + '/api/v1/devices/' + conf['UUID'])
    if (r.status_code == 200 and json.loads(r.content)['totalRecords'] == 1):
        dataToSend = json.loads(r.content)['records'][0]
        if (dataToSend['labels'] is None):
            dataToSend['labels'] = {}
        dataToSend['labels'][labelKey] = labelValue
        r = httpPut(conf['server'] + '/api/v1/devices/' + conf['UUID'], dataToSend)
        if (r.status_code == 200):
            return r
        else:
            log("Issue while updating device label on server: " + str(r.status_code))
            log(r.content)    
            return r
    else:
        log("Issue while getting device label from server: " + str(r.status_code))
        log(r.content)
        return r
    return


def getMilliSecondsSinceEpoch():
    return int((datetime.utcnow() - datetime(1970,1,1)).total_seconds() * 1000)


# Is a string valid json
def isJson(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


# For a file in the current dir, get the absolute location
def getAbsoluteFileLocation(inputFilename):
    return os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), inputFilename)


# Remove a directory if it already exists and make it again, ready for writing to
def provideFreshDirectory(dirToClean):
    if(len(dirToClean) > 3):
        os.system("sudo rm -rf " + dirToClean)
        os.system("mkdir -p " + dirToClean)
        os.system("sudo chmod 777 " + dirToClean)
    return


# Ensure a directory exists for writing to
def ensureDirectoryExists(dir):
    if(len(dir) > 3):
        os.system("mkdir -p " + dir)
        os.system("sudo chmod 777 " + dir)
    return


# Edit a json file to upsert a parameter
def readJsonFile(filename):
    with stateLock:
        with open(filename) as data_file:
            return json.load(data_file)


def writeJsonAtomic(filename, value):
    """Publish a complete Agent state file; never a truncated intermediate JSON."""
    with stateLock:
        directory = os.path.dirname(os.path.abspath(filename))
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".agent-state-", dir=directory)
        try:
            with os.fdopen(fd, "w") as outfile:
                json.dump(value, outfile, indent=4)
                outfile.flush()
                os.fsync(outfile.fileno())
            os.replace(temporary, filename)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def upsertJsonEntries(filename, updates, expectedFunctionUuid=None):
    with stateLock:
        content = readJsonFile(filename)
        if expectedFunctionUuid is not None and content.get("functionParameterValues", {}).get("functionUuid") != expectedFunctionUuid:
            raise ValueError("function_identity_mismatch")
        content.update(updates)
        writeJsonAtomic(filename, content)
        return content


def upsertJsonEntry(jsonFileToEdit, jsonKey, jsonValue):
    return upsertJsonEntries(jsonFileToEdit, {jsonKey: jsonValue})


# Reduce a string to only safe characters
def safeChars(inputStr):
    return(''.join(ch for ch in inputStr if ch.isalnum()))


def generateUuid():
    return str(uuid.uuid4())

