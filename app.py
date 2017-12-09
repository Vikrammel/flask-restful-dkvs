#-*- coding: utf-8 -*-
#app.py for project 4, CMPS128

from flask import Flask
from flask import request, abort, jsonify, json
from flask_restful import Resource, Api
import re, sys, os, requests, datetime, threading, random, bisect
from hashlib import md5

app = Flask(__name__)
api = Api(app)
newline = "\n"
# Debug printing boolean.
debug = True
# Array to fill with print IDs to ignore when debugging. Add 'Hb' to ignore heartBeat prints, etc.
ignorePrints = []
firstHeartBeat = True

def _print(text, id=''):
    if debug:
        if id in ignorePrints:
            return
        print (text)
        sys.stdout.flush()

# Get expected environment variables.
IpPort = os.environ.get('IPPORT')
try:
    K = int(os.environ.get('K'))
except:
    K = ''
halfNode = False
try:
    EnvView = os.environ.get('VIEW')
except:
    EnvView = ''
    halfNode = True

# Is this a replica or a proxy?
isReplica = True
# String to prepend onto URL.
http_str = 'http://'
kv_str = '/kv-store/'

numberOfPartitions = -1
partition = -1
# Dictionary where key->value pairs will be stored.
d = {}
cDict = {}
# Dictionaries acting as a vector clock and timestamps. key -> local clock value/timestamp.
vClock = {}
storedTimeStamp = {}
hashRing=[] #sorted ring of hashes
hashClusterMap={} #maps buckets to clusters
# Arrays to store replicas and proxies.
view = []
notInView = [] # Keep track of nodes not in view to see if they're back online.
replicas = []
proxies = []

def getReplicaDetail():
    global isReplica
    return isReplica

def printDict():
    for k in d:
        print(str(k) + newline + sttr(d[k]))
    sys.stdout.flush()

def setReplicaDetail(number):
    global isReplica
    if int(number) == 1:
        isReplica = True
    elif int(number) == 0:
        isReplica = False
    _print("isReplica set to " + str(isReplica), 'Rd')

# Cleans empty strings out of our arrays...
def cleanArrays():
    if len(view) > 0
        while view[0] == '':
            view.pop(0)
            if len(view) == 0:
                break
    if len(replicas) > 0
        while replicas[0] == '':
            replicas.pop(0)
            if len(replicas) == 0:
                break
    if len(proxies) > 0
        while proxies[0] == '':
            proxies.pop(0)
            if len(proxies) == 0:
                break
    if len(notInView) > 0
        while notInView[0] == '':
            notInView.pop(0)
            if len(notInView) == 0:
                break

#function to order list of IPs so that nodes can refrence the same node by index when using the
#array for list, replicas, or proxies
#TODO: Use something more efficient than insertion sort.
def sortIPs(IPArr):
    #use checksum to order IPs; sum nums in IP and store in ascending order
    AnsArr = IPArr
    def sum_digits(digit):
        return sum(int(x) for x in digit if x.isdigit())
    for i in range(1, len(AnsArr)):
        ip = AnsArr[i]
        ipSum = sum_digits(AnsArr[i])
        # insertion sort
        j = i-1
        while j >=0 and ipSum < sum_digits(AnsArr[j]):
            AnsArr[j+1] = AnsArr[j]
            j -= 1
        AnsArr[j+1] = ip
    return AnsArr

# Initialize view array based on Environment Variable 'VIEW'
if EnvView is not None:
    notInView = EnvView.split(",")
    notInView = sortIPs(notInView)
if IpPort in notInView:
    notInView.remove(IpPort)
view.append(IpPort)

def removeReplica(ip):
    replicas.remove(ip)
    view.remove(ip)

def removeProxie(ip):
    proxies.remove(ip)
    view.remove(ip)

def die(exitCode):
    sys.exit(exitCode)

def heartBeat():
    heart = threading.Timer(3.0, heartBeat)
    heart.daemon = True
    heart.start()
    global firstHeartBeat, replicas, proxies, view, notInView
    if firstHeartBeat:
        firstHeartBeat = False
        return

    _print("My IP: " + str(IpPort) + newline +
        "View: " + str(view) + newline +
        "Replicas: " + str(replicas) + newline +
        "Proxies: " + str(proxies), 'Hb')

    for ip in notInView: #check if any nodes not currently in view came back online
        try:
            response = (requests.get((http_str + ip + kv_str + "get_node_details"), timeout=2)).json()
            if response['result'] == 'success':
                notInView.remove(ip)
                view.append(ip)
                view = sortIPs(view)
        except: #Handle no response from i
            pass
    for ip in view:
        if ip != IpPort:
            try:
                response = (requests.get((http_str + ip + kv_str + "get_node_details"), timeout=2)).json()
            except requests.exceptions.RequestException as exc: #Handle no response from ip
                if ip in replicas:
                    removeReplica(ip)
                elif ip in proxies:
                    removeProxie(ip)
                notInView.append(ip)
                notInView = sortIPs(notInView)
    #updateHashRing()
    updateRatio()
    _print("reps " + str(replicas), 'Hb')
    _print("prox " + str(proxies), 'Hb')

def updateHashRing():
    global cDict, K, view, hashClusterMap, hashRing
    cSet = set()
    hasher = md5()

    #sort view and update cDict off of view
    view = sortIPs(view)
    for node in view:
        clusterNum = view.index(node)/int(K)
        cDict[node] = clusterNum

    #make a set of unique vals (which are clusterNumbers) from cDict
    for val in cDict.values():
        cSet.add(val)

    #traverse through set to generate 250 buckets for each clusters
    hashRing = []
    hashClusterMap = []
    for cIndex in cSet:
        for j in range(250): #generate 250 buckets per cluster
            hasher.update("hash string" + str(cIndex) + str(j))
            bisect.insort(hashRing, hasher.hexdigest())
            hashClusterMap[hasher.hexdigest()] = cIndex

def updateDatabase():
    global replicas, notInView
    for ip in replicas:
    updateHashRing() #initially called it here, seemes excessive,
    # calling only when view[] and notInView change now (in heartbeat)
        if ip == IpPort:
            continue
        try:
            response = (requests.get((http_str + ip + kv_str + '_getAllKeys!'), timeout=5)).json()
            try:
                responseD = json.loads(response['dict'])
            except:
                _print("Can't get data from a halfNode", 'Db')
                continue
            responseCausal = json.loads(response['causal_payload'])
            responseTime = json.loads(response['timestamp'])
            for key in responseD:
                if (d.get(key) == None or responseCausal[key] > vClock[key] or
                   (responseCausal[key] == vClock[key] and responseTime[key] > storedTimeStamp[key]) or
                   (responseTime[key] == storedTimeStamp[key] and responseD[key] > d[key])):
                    d[key] = responseD[key].encode('ascii', 'ignore')
                    vClock[key] = responseCausal[key]
                    storedTimeStamp[key] = responseTime[key]
        except requests.exceptions.RequestException: #Handle no response from ip
            _print("updateDatabase timeout occured.", 'Db')
            removeReplica(ip)
            notInView.append(ip)
            notInView = sortIPs(notInView)

def updateView(self, key):
    global replicas, proxies, view, notInView, K, halfNode
    if halfNode == True:
        halfNode = False
    # Special condition for broadcasted changes.
    try:
        sysCall = request.form['_systemCall']
        ip_payload = request.form['ip_port'].encode('ascii', 'ignore')
        _type = request.form['type']
    # Normal updateView call.
    except:
        sysCall = ''
        # Checks to see if ip_port was given in the data payload
        try:
            ip_payload = request.form['ip_port'].encode('ascii', 'ignore')
        except:
            ip_payload = ''

        # If payload is empty
        if ip_payload == '':
            return {'result': 'error', 'msg': 'Payload missing'}, 403

        # Checks to see if request parameter 'type' was given, and what its value is set to
        try:
            _type = request.args.get('type')
        except: 
            _type = ''

    if sysCall == '':
        for address in view:
            if address == IpPort or address == ip_payload:
                continue
            try:
                requests.put((http_str + address + kv_str + 'update_view'),
                    data = {'ip_port': ip_payload, 'type': _type, '_systemCall': True})
            except:
                pass

    if _type == 'add':
        # Check if IP is already in our view
        if ip_payload in view:
            return {'result': 'error', 'msg':'IP already inside view'}, 403
        view.append(ip_payload)
        view = sortIPs(view)
        if sysCall == '':
            requests.put(http_str + ip_payload + kv_str + '_update!', 
                data = {"K": K, "view": ','.join(view), "notInView": ','.join(notInView), "proxies": ','.join(proxies)})
        updateRatio()
        return {"result": "success", "partition_id": int(view.index(ip_payload)/K), 
            "number_of_partitions": int(len(view)/K)}, 200

    elif _type == 'remove':
        # Check to see if IP is in our view
        if ip_payload not in view:
            if ip_payload not in notInView:
                return {'result': 'error', 'msg': 'Cannot remove, IP is not in view'}, 403
            else:
                notInView.remove(ip_payload)
                return {"result": "success", "number_of_partitions": int(len(view)/K)}, 200
        
        # Check if replica
        if ip_payload in replicas:
            removeReplica(ip_payload)
        # Check if proxie
        if ip_payload in proxies:
            removeProxie(ip_payload)
        
        if ip_payload in view:
            view.remove(ip_payload)
        if ip_payload in notInView:
            notInView.remove(ip_payload)
        
        # requests.put(http_str + ip_payload + kv_str + '_die!', data = {})
        # Update Replica/Proxie Ratio if needed
        updateRatio()
        return {"result": "success", "number_of_partitions": int(len(view)/K)}, 200
    return {'result': 'error', 'msg': 'Request type not valid'}, 403

def updateRatio():
    global view, replicas, proxies, partition
    cleanArrays()
    view = sortIPs(view)

    newPartition = int(view.index(IpPort)/K)
    proxyPartition = int(len(view)/K)

    if len(view) >= K:
        _print("partition: " + str(partition) + newline +
                "newPartition: " + str(newPartition) + newline +
                "proxy partition: " + str(proxyPartition), 'Ur')
        # The partition this node belongs to had changed.
        if partition != newPartition:
            partition = newPartition
            replicas = []
            d = {} # Potentially dangerous. Alternative?
            if partition >= proxyPartition:
                if IpPort not in proxies:
                    setReplicaDetail(0)
                    _print("Converted to " + str(getReplicaDetail()) + " at 0.5", 'Ur')
                    proxies.append(IpPort)
                    proxies = sortIPs(proxies)
            else:
                setReplicaDetail(1)
                _print("Converted to " + str(getReplicaDetail()) + " at 1", 'Ur')

    for node in view:
        # This is a proxy.
        if int(view.index(node)/K) >= proxyPartition:
            if node not in proxies:
                if node in replicas:
                    replicas.remove(node)
                proxies.append(node)
                proxies = sortIPs(proxies)
                if node == IpPort:
                    setReplicaDetail(0)
                    _print("Converted to " + str(getReplicaDetail()) + " at 2", 'Ur')
                    
        # This is a replica within the same partition.
        elif getReplicaDetail() and int(view.index(node)/K) == partition:
            if node not in replicas:
                if node in proxies:
                    proxies.remove(node)
                replicas.append(node)
                replicas = sortIPs(replicas)
                if node == IpPort:
                    updateDatabase()
        else:
            if node in proxies:
                proxies.remove(node)
            if node in replicas:
                replicas.remove(node)

#read-repair function
def readRepair(key):
    global replicas, notInView
    for ip in replicas:
        if ip == IpPort:
            continue
        try:
            response = requests.get((http_str + ip + kv_str + key), timeout=2)
            try:
                responseD = response['value']
            except:
                continue
            responseCausal = response['causal_payload']
            responseTime = response['timestamp']
            if (d.get(key) == None or responseCausal > vClock[key] or
               (responseCausal == vClock[key] and responseTime > storedTimeStamp[key]) or
               (responseTime == storedTimeStamp[key] and responseD > d[key])):
                d[key] = responseD[key].encode('ascii', 'ignore')
                vClock[key] = responseCausal[key]
                storedTimeStamp[key] = responseTime[key]
        except requests.exceptions.RequestException: #Handle no response from ip
            removeReplica(ip)
            notInView.append(ip)
            notInView = sortIPs(notInView)

def broadcastKey(key, value, payload, time):
    global replicas
    for address in replicas:
        if address != IpPort:
            try:
                _print("Sending to " + str(address), 'Bc')
                requests.put((http_str + address + kv_str + key), 
                    data = {'val': value, 'causal_payload': payload, 'timestamp': time, 'bc': '1'})
            except :
                _print("broadcast failed to " + str(address), 'Bc')
                # removeReplica(address)

def getPartition(num):
    partitionStart = num*K
    partitionEnd = partitionStart + K
    if partitionEnd > len(view):
        return None
    membersRange = range(partitionStart, partitionEnd)
    members = []
    for node in view:
        if view.index(node) in membersRange:
            members.append(node)
    return members

class Handle(Resource):
    #Handles GET request
    def get(self, key):
        if getReplicaDetail():
            _print("Replica request\n\n", 'Gr')
            global d, vClock, storedTimeStamp, halfNode
            
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                return {"result": "success", "replica": "Yes"}, 200
            #Special command: Returns the ID of the partition this replica is in.
            if key == 'get_partition_id':
                return {"result": "success", "partition_id": partition}, 200
            #Special command: Returns list of partition IDs.
            if key == 'get_all_partition_ids':
                return {"result": "success", "partition_id_list": range(0, int(len(view)/K))}, 200
            #Special command: Returns list of replicas in the given partition.
            if key == 'get_partition_members':
                try:
                    partition_id = int(request.form['partition_id'])
                except:
                    return {"result": "error", 'msg': 'Partition not specified'}, 403
                members = getPartition(partition_id)
                if members is None:
                    return {"result": "error", 'msg': 'Partition does not exist'}, 403
                return {"result": "success", "partition_members": members}, 200
            #Special command: Get the entire local dictionary, vector clock and timestamps included.
            if key == '_getAllKeys!':
                if halfNode == True:
                    return {"result": "half"}, 207
                return {"result": "success", "dict": json.dumps(d), "causal_payload": json.dumps(vClock),
                    "timestamp": json.dumps(storedTimeStamp)}, 200

            #If key is not in dict, return error.
            if key not in d:
                readRepair(key)
                if key not in d:
                    return {'result': 'error', 'msg': 'Key does not exist'}, 404

            clientRequest = False
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
            if timestamp is '':
                clientRequest = True

            # Get timestamp, if it exist
            if key in storedTimeStamp:
                timestamp = storedTimeStamp[key]
            
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
                pass
            if causalPayload is None:
                if vClock[key] is None:
                    vClock[key] = 0
            #Increment vector clock when client get operation succeeds.
            if clientRequest:
                vClock[key] += 1
            #If key is in dict, return its corresponding value.
            return {'result': 'success', 'value': d[key], 'partition_id': partition, 
                'causal_payload': vClock[key], 'timestamp': timestamp}, 200
        #----------------------------------------------------------------------------------------------------#
        else:
            _print("Proxy request\n\n", 'Gp')
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                return {"result": "success", "replica": "No"}, 200
            #Special command: Returns -1, since this is a proxy, not a replica.
            if key == 'get_partition_id':
                return {"result": "success", "partition_id": -1}, 200

            #Try to retrieve timestamp and cp of read request.
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
                pass
            #Try requesting random replicas
            noResp = True
            dataCluster = getPartition(0) # TODO: Change this to direct to correct partition rather then '0'.
            while noResp:
                if dataCluster is None:
                    return {'result': 'error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(dataCluster)
                try:
                    response = requests.get(http_str + repIp + kv_str + key,
                        data = {'causal_payload': causalPayload, 'timestamp': timestamp})
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    dataCluster.remove(repIp)
                    view.remove(repIp)
                    notInView.append(repIp)
                    notInView = sortIPs(notInView)
                    continue
                noResp = False
            _print(response.json(), 'Gp')
            return response.json()

    #Handles PUT request
    def put(self, key):
        if getReplicaDetail():
            global K, view, notInView, replicas, proxies, d, vClock, storedTimeStamp
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)
            #Special command: Kill this node dead.
            if key == '_die!':
                die(0)
                return {"result": "error", 'msg': 'Node at '+IpPort+' failed to die.'}, 500
            #Special command: View update.
            if key == '_update!':
                try:
                    K = int(request.form['K'].encode('ascii', 'ignore'))
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")

                except:
                    return {"result": "error", 'msg': 'System command parameter error'}, 403

                cleanArrays()
                return {"result": "success"}, 200

            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                try:
                    replicaDetail = request.form['id']
                except:
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403
                _print("Converted to " + str(replicaDetail), 'Pr')
                setReplicaDetail(int(replicaDetail))
                _print("Converted to " + str(getReplicaDetail()) + " at 4", 'Pr')
                return {"result": "success"}, 200

            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val'].encode('ascii', 'ignore')
            except:
                return {'result': 'error', 'msg': 'No value provided'}, 403
            try:
                causalPayload = int(request.form['causal_payload'].encode('ascii', 'ignore'))
            except:
                causalPayload = ''
            try:
                key = key.encode('ascii', 'ignore')
            except:
                _print("Could not encode key.", 'Pr')

            #Restricts key length to 1<=key<=200 characters.
            if not 1 <= len(str(key)) <= 200:
                return {'result': 'error', 'msg': 'Key not valid'}, 403
            #Restricts key to alphanumeric - both uppercase and lowercase, 0-9, and _
            if not re.match(r'^\w+$', key):
                return {'result': 'error', 'msg': 'Key not valid'}, 403

            #Restricts value to a maximum of 1Mbyte.
            if sys.getsizeof(value) > 1000000:
                return {'result': 'error', 'msg': 'Object too large. Size limit is 1MB'}, 403

            clientRequest = False
            #Get attached timestamp, or set it if empty.
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
            if timestamp is '':
                tempTime = datetime.datetime.now()
                timestamp = (tempTime-datetime.datetime(1970,1,1)).total_seconds()
                clientRequest = True

            responseCode = 200
            if key not in vClock:
                vClock[key] = 0
                responseCode = 201
            if key not in storedTimeStamp:
                storedTimeStamp[key] = 0
            
            #If causal payload is none, and replica key is none initialize payload to 0 and set key value.
            if causalPayload is '':
                causalPayload = vClock[key]
            if causalPayload >= vClock[key]:
                #Actually set the value
                if causalPayload == vClock[key]:
                    if key in storedTimeStamp:
                        if storedTimeStamp[key] < timestamp:
                            d[key] = value
                        elif storedTimeStamp[key] == timestamp:
                            if d[key] < value:
                                d[key] = value
                #Handle early put requests.
                else:
                    d[key] = value
                    vClock[key] = causalPayload
                    storedTimeStamp[key] = timestamp

                if clientRequest == True:
                    #Increment vector clock when client put operation succeeds.
                    vClock[key] += 1
                    storedTimeStamp[key] = timestamp
                    broadcastKey(key, value, vClock[key], storedTimeStamp[key])
                return {'result': 'success', 'partition_id': partition, 'causal_payload': vClock[key],
                    'timestamp': storedTimeStamp[key]}, responseCode
            #If key already exists, set replaced to true.
            if storedTimeStamp[key] == 0:
                storedTimeStamp[key] = timestamp
            return {'result': 'success', 'partition_id': partition, 'causal_payload': vClock[key],
                'timestamp': storedTimeStamp[key]}, responseCode
        #----------------------------------------------------------------------------------------------------#
        else:
            global replicas
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)
            #Special command: View update.
            if key == '_update!':
                try:
                    K = int(request.form['K'].encode('ascii', 'ignore'))
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")
                except:
                    return {"result": "error", 'msg': 'System command parameter error'}, 403

                cleanArrays()
                return {"result": "success"}, 200
            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                try:
                    replicaDetail = request.form['id']
                except:
                    _print("'ID not provided in setIsReplica", 'Pp')
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403
                setReplicaDetail(int(replicaDetail))
                _print("Converted to " + str(getReplicaDetail()) + " at 8", 'Pp')
                return {"result": "success"}, 200

            #Makes sure a value was actually supplied in the PUT.
            try:
                timestamp = request.form['timestamp'].encode('ascii', 'ignore')
            except:
                timestamp = ''
            try:
                value = request.form['val'].encode('ascii', 'ignore')
            except:
                return {"result": "error", 'msg': 'No value provided'}, 403
            try:
                causalPayload = request.form['causal_payload'].encode('ascii', 'ignore')
            except:
                causalPayload = ''
            try:
                key = key.encode('ascii', 'ignore')
            except:
                _print("Key not encoding", 'Pp')
                pass
            #Try requesting random replicas
            noResp = True
            dataCluster = getPartition(0) # TODO: Change this to direct to correct partition rather then '0'.
            while noResp:
                if dataCluster is None:
                    return {'result': 'error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(dataCluster)
                try:
                    response = requests.put((http_str + repIp + kv_str + key), 
                        data = {'val': value, 'causal_payload': causalPayload })
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    dataCluster.remove(repIp)
                    view.remove(repIp)
                    notInView.append(repIp)
                    notInView = sortIPs(notInView)
                    continue
                noResp = False
            _print(response.json(), 'Pp')
            return response.json()

api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
    _print("IPPORT: " + str(IpPort))
    localAddress = IpPort.split(":")
    _print("Local Address:" + str(localAddress))
    heartBeat()
    app.run(host=localAddress[0], port=int(localAddress[1]), threaded=True)