#-*- coding: utf-8 -*-
#app.py for project 3, CMPS128

from flask import Flask
from flask import request, abort, jsonify, json
from flask_restful import Resource, Api
import re, sys, os, requests, datetime, threading, random

app = Flask(__name__)
api = Api(app)
newline = "&#13;&#10;"
# Debug printing boolean.
debug = True
firstHeartBeat = True

# Get expected environment variables.
IpPort = os.environ.get('IPPORT')
try:
    K = int(os.environ.get('K'))
except:
    K = ''
try:
    EnvView = os.environ.get('VIEW')
except:
    EnvView = ''

# Is this a replica or a proxy?
isReplica = True
# Dictionaries acting as a vector clock and timestamps. key -> local clock value/timestamp.
vClock = {}
storedTimeStamp = {}
# String to prepend onto URL.
http_str = 'http://'
kv_str = '/kv-store/'

numberOfPartitions = -1
partitionID = -1
localCluster = []
# Dictionary where key->value pairs will be stored.
d = {}
cDict = {}
# Arrays to store replicas and proxies.
view = []
notInView = [] # Keep track of nodes not in view to see if they're back online.
replicas = []
proxies = []

def getReplicaDetail():
    return isReplica

def printDict():
    for k in d:
        print(str(k))
        print(sttr(d[k]))
        sys.stdout.flush()

def setReplicaDetail(number):
    if int(number) == 1:
        isReplica = True
    elif int(number) == 0:
        isReplica = False

#function to order list of IPs so that nodes can refrence the same node by index when using the
#array for list, replicas, or proxies
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
        while j >=0 and ipSum < sum_digits(AnsArr[j]) :
            AnsArr[j+1] = AnsArr[j]
            j -= 1
        AnsArr[j+1] = ip
    return AnsArr


if EnvView is not None:
    print("Environment variable 'View' is not none")
    sys.stdout.flush()
    view = EnvView.split(",")
    numberOfPartitions = int(len(view) / K)
    print("Number of partitions = " + str(numberOfPartitions))
    sys.stdout.flush()
    tempArr = []
    counter = 0
    clusterCounter = 0

    # loops through each address
    for address in view:

        # Checks to see if number of clusters have been fulfilled
        # adds the rest of the address to proxies
        if clusterCounter == numberOfPartitions:
            proxies.append(address)

        # appends address to temporary array
        tempArr.append(address)
        counter += 1

        # Sets cluster number
        if address == IpPort:
            partitionID = clusterCounter
            print("My Cluster number is " + str(partitionID))
        # Once counter reaches K, set the cluster dictionary with the temp array
        # Increase cluster dictionary position, reset temp array, and counter
        if counter == K:
            cDict[clusterCounter] = tempArr
            tempArr = []
            counter = 0
            clusterCounter += 1

        # Sets local cluster and isReplica = True, if not a proxy
    if partitionID != -1:
        replicas = cDict[partitionID]
        isReplica = True
    else:
        isReplica = False
    
    print(cDict)
    print(proxies)
    sys.stdout.flush()
        

# Initialize view array based on Environment Variable 'VIEW'
# if EnvView is not None:
#     print("env view is not none")
#     sys.stdout.flush()
#     view = EnvView.split(",")
#     for i in view:
#         if view.index(i) < K:
#             replicas.append(i)
#             replicas = sortIPs(replicas)
#         else:
#             proxies.append(i)
#             proxies = sortIPs(proxies)
        
#     if IpPort in replicas:
#         isReplica = True
# else:
#     print("env view is none")
#     sys.stdout.flush()
#     view = []
#     view.append(IpPort)

# def removeReplica(ip):
#     replicas.remove(ip)
#     view.remove(ip)

# def removeProxie(ip):
#     proxies.remove(ip)
#     view.remove(ip)

# def heartBeat():
#     heart = threading.Timer(3.0, heartBeat)
#     heart.daemon = True
#     heart.start()
#     global firstHeartBeat, replicas, proxies, view, notInView
#     if firstHeartBeat:
#         firstHeartBeat = False
#         return
#     if debug:
#         # print("My IP: " + str(IpPort))
#         print("View: " + str(view))
#         print("Replicas: " + str(replicas))
#         print("Proxies: " + str(proxies))
#         sys.stdout.flush()

#     for ip in notInView: #check if any nodes not currently in view came back online
#         # print("for notinView loop eneter")
#         # sys.stdout.flush()
#         try:
#             response = (requests.get((http_str + ip + kv_str + "get_node_details"), timeout=2)).json()
#             if response['result'] == 'success':
#                 notInView.remove(ip)
#                 view.append(ip)
#                 view = sortIPs(view)
#         except: #Handle no response from i
#             pass
#     for ip in view:
#         # print("For IP loop Entered")
#         # sys.stdout.flush()
#         if ip != IpPort:
#             # print("ip not equal to IPPort")
#             # sys.stdout.flush()
#             try:
#                 response = (requests.get((http_str + ip + kv_str + "get_node_details"), timeout=2)).json()
#             except requests.exceptions.RequestException as exc: #Handle no response from ip
#                 # print("Try Failed, now inside except")
#                 # sys.stdout.flush()
#                 if ip in replicas:
#                     # print("in is in replicas")
#                     # sys.stdout.flush() 
#                     removeReplica(ip)
#                 elif ip in proxies:
#                     # print("ip is in proxies")
#                     # sys.stdout.flush()
#                     removeProxie(ip)
#                 notInView.append(ip)
#                 notInView = sortIPs(notInView)
#     updateRatio()

def updateView(self, key):
    global firstHeartBeat, replicas, proxies, view, notInView, K, IpPort
    # Special condition for broadcasted changes.
    try:
        sysCall = request.form['_systemCall']
        ip_payloadU = request.form['ip_port']
        ip_payload = ip_payloadU.encode('ascii', 'ignore')
        _type = request.form['type']
    
    # Normal updateView call.
    except:
        sysCall = ''
        # Checks to see if ip_port was given in the data payload
        try:
            ip_payloadU = request.form['ip_port']
            ip_payload = ip_payloadU.encode('ascii', 'ignore')
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
            if address != IpPort and address != ip_payload:
                try:
                    requests.put((http_str + address + kv_str + 'update_view'), data = {'ip_port': ip_payload, 'type': _type, '_systemCall': True})
                except:
                    print("failed to update view to address: " + address)
                    sys.stdout.flush()

    if _type == 'add':
        # Check if IP is already in our view
        if ip_payload in view:
            try:
                rJ = requests.get(http_str + ip_payload + kv_str + 'get_all_replicas')
                response = rJ.json()
            except:
                print("Node not responding")
                sys.stdout.flush()
                return {'result': 'error, node not responding on add'}, 500
            try:
                numRep = len(response['replicas'])
                print(str(response['replicas']) + " length = " + str(numRep))
                sys.stdout.flush()
            except:
                numRep = 0

            print ("numrep: " + str(numRep))
            sys.stdout.flush()

            # if numrep is 0, that means this is  a new node, but is already inside this nodes view array from an old node
            # so we have to pass it the view, K, replicas, proxies, arrays
            # if numRep > 0:
            return {'result': 'success', 'node_id': str(ip_payload), 'number_of_nodes': str(len(view))}, 200
            # else:
            #     headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
            #     requests.put(http_str + ip_payload + kv_str + '_update!', data = {"K": K, "view": ','.join(view), "notInView": ','.join(notInView), "replicas": ','.join(replicas), "proxies": ','.join(proxies)})
            #     return {'result': 'success', 'node_id': str(ip_payload), 'number_of_nodes': str(len(view))}, 212

        if debug:
            print(K)
            print(len(replicas))
        if ip_payload != IpPort:
            if len(replicas) < K:
                # Creates new replica
                replicas.append(ip_payload)
                view.append(ip_payload)
                replicas = sortIPs(replicas)
                view = sortIPs(view)  
                requests.put(http_str + ip_payload + kv_str + '_setIsReplica!', data={'id': 1, 'd': json.dumps(d), 'vClock': json.dumps(vClock), 'storedTimeStamp': json.dumps(storedTimeStamp)}) #tell node to be replica
                if debug:
                    print("New replica created.")
                    sys.stdout.flush()
            
            else:
                # Creates new proxy
                proxies.append(ip_payload)
                view.append(ip_payload)
                proxies = sortIPs(proxies)
                view = sortIPs(view)  
                if debug:
                    print("New proxie created.")
                    sys.stdout.flush()
        
        if ip_payload in notInView:
            notInView.remove(ip_payload)

        updateRatio()  
        if ip_payload != IpPort: 
            requests.put(http_str + ip_payload + kv_str + '_update!', data = {"K": K, "view": ','.join(view), "notInView": ','.join(notInView), "replicas": ','.join(replicas), "proxies": ','.join(proxies)})
        return {"msg": "success", "node_id": ip_payload, "number_of_nodes": len(view)}, 200

    if _type == 'remove':
        # Check to see if IP is in our view
        if ip_payload not in view:
            if ip_payload in notInView:
                notInView.remove(ip_payload)
                return {"msg": "success", "number_of_nodes": len(view)}, 200
            return {'result': 'error', 'msg': 'Cannot remove, IP is not in view'}, 403
        
        # Check if replica
        if ip_payload in replicas:
            removeReplica(ip_payload)
        # Check if proxie
        if ip_payload in proxies:
            removeProxie(ip_payload)
        # Update Replica/Proxie Ratio if needed
        updateRatio()

        if ip_payload in notInView:
            notInView.remove(ip_payload)
        return {"msg": "success", "number_of_nodes": len(view)}, 200
    return {'result': 'error', 'msg': 'Request type not valid'}, 403

# def updateRatio():
#     global view, replicas, proxies, isReplica
#     view = sortIPs(view)
#     for node in view:
#         # This is a replica.
#         if view.index(node) < K:
#             #print("UpdateRatio: " + node + "is in pos < K")
#             sys.stdout.flush()
#             if node not in replicas:
#                 if node in proxies:
#                     proxies.remove(node)
#                 replicas.append(node)
#                 replicas = sortIPs(replicas)
#                 #print("UpdateRatio: " + node + "added to replicas array")
#                 sys.stdout.flush()
#                 if node == IpPort:
#                     isReplica = True
#                     for key in d:
#                         readRepair(key)
#         # This is a proxy.
#         else:
#             #print("UpdateRatio: " + node + "is in pos >= K")
#             sys.stdout.flush()
#             if node not in proxies:
#                 if node in replicas:
#                     replicas.remove(node)
#                 proxies.append(node)
#                 proxies = sortIPs(proxies)
#                 #print("UpdateRatio: " + node + "added to replicas array")
#                 sys.stdout.flush()
#                 if node == IpPort:
#                     isReplica = False
   
#read-repair function
def readRepair(key):
    pass
    global firstHeartBeat, replicas, proxies, view, notInView
    for ip in replicas:
        try:
            response = requests.get((http_str + ip + kv_str + key), timeout=2)
            if response[causal_payload] > vClock[key]:
                d[key] = response[value]
                vClock[key] = response[causal_payload]
                timestamps[key] = response[timestamp]
        except requests.exceptions.RequestException: #Handle no response from ip
            removeReplica(ip)
            notInView.append(ip)
            notInView = sortIPs(notInView)
def broadcastKey(key, value, payload, time):
    global firstHeartBeat, replicas, proxies, view, notInView
    for address in replicas:
        if address != IpPort:
            # print("Address: " + str(address)+ " Address type: " + str(type(address)))
            # print("IpPort: " + str(IpPort)+ " IpPort type: " + str(type(IpPort)))
            # print("KEY: " + str(key)+ " Address type: " + str(type(key)))
            # print("value: " + str(value)+ " value type: " + str(type(value)))
            # print("payload: " + str(payload)+ " payload type: " + str(type(payload)))
            # print("time: " + str(time)+ " time type: " + str(type(time)))
            sys.stdout.flush()
            try:
                print("Sending to " + str(address))
                sys.stdout.flush()
                requests.put((http_str + address + kv_str + key), data = {'val': value, 'causal_payload': payload, 'timestamp': time, 'bc':'1'})
            except :
                print("broadcast failed to " + str(address))
                sys.stdout.flush()
                # removeReplica(address)

class Handle(Resource):
    isReplica = getReplicaDetail
    if isReplica:
        #Handles GET request
        def get(self, key):
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                answer = "No"
                if isReplica == True:
                    answer = "Yes"
                return {"result": "success", "replica": answer}, 200
            #Special command: Returns list of replicas.
            if key == 'get_all_replicas':
                return {"result": "success", "replicas": replicas}, 200

             #Special command: Returns the ID of the partition this replica is in.
            if key == 'get_partition_id':
                return {"result": "success", "partition_id": partitionID}, 200

            #Special command: Returns list of partition IDs.
            if key == 'get_all_partition_ids':
                partitionIds = []
                for k in cDict:
                    partitionIds.append(k)
                return {"result": "success", "partition_id_list": partitionIds}, 200

            #Special command: Returns list of replicas in the given partition.
            if key == 'get_partition_members':
                try:
                    partition_id = int(request.form['partition_id'])
                except:
                    return {"result": "error", 'msg': 'Partition not specified'}, 403
                #TODO: Return members of the given partition, rather then simply the local one.
                if partition_id in cDict:
                    return {"result": "success", "partition_members": cDict[partition_id]}, 200
                else:
                    return {"result": "error", 'msg': 'Partition not specified'}, 403

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
            return {'result': 'success', 'value': d[key], 'partition_id': partitionID, 'causal_payload': vClock[key], 'timestamp': timestamp}, 200

        #Handles PUT request
        def put(self, key):
            global K, view, notInView, replicas, proxies, d, vClock, storedTimeStamp
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)

            if key == 'bc':
                try:
                    print("1!!!!!!!!!")
                    sys.stdout.flush()
                except:
                    pass
                try:
                    value = request.form['val'].encode('ascii', 'ignore')
                    print("2!!!!!!!!!")
                    sys.stdout.flush()
                except:
                    value = 'test'
                try:
                    causalPayload = int(request.form['causal_payload'].encode('ascii', 'ignore'))
                    print("3!!!!!!!!!")
                    sys.stdout.flush()
                except:
                    causalPayload = 'test'
                try:
                    dKey = request.form['key'].encode('ascii', 'ignore')
                    print("4!!!!!!!!!")
                    sys.stdout.flush()
                except:
                    dKey = 'test'
                try:
                    timestamp = request.form['timestamp']
                    print("5!!!!!!!!!")
                    sys.stdout.flush()
                except:
                    timestamp = 'test'
                    
                d[dKey] = value
                vClock[key] = causalPayload
                storedTimeStamp[key] = timestamp
                return {"result":"success"}, 269



            print("TEST!!!!!!!!!")
            sys.stdout.flush()

            #Special command: Force read repair and view update.
            if key == '_update!':
                try:
                    K = int(request.form['K'].encode('ascii', 'ignore'))
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    replicas = request.form['replicas'].encode('ascii', 'ignore').split(",")
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")

                except:
                    return {"result": "error", 'msg': 'System command parameter error'}, 403

                if proxies[0] == '':
                    proxies.pop(0)
                if replicas[0] == '':
                    replicas.pop(0)
                if notInView[0] == '':
                    notInView.pop(0)
                if view[0] == '':
                    view.pop(0)
                
                if isReplica:
                    for key in d:
                        readRepair(key)
                

                return {"result": "success"}, 200
                
            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                try:
                    uni = request.form['id']
                    replicaDetail = uni.encode('ascii', 'ignore')
                except:
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403

                setReplicaDetail(uni)

                try: 
                    dt = json.loads(request.form['d'])
                    vt = json.loads(request.form['vClock'])
                    st = json.loads(request.form['storedTimeStamp'])
                    print("type of dt = " + str(type(dt)))
                    print(dt)
                    print(vt)
                    print(st)
                    sys.stdout.flush()
                    d = dt
                    vClock = vt
                    storedTimeStamp = st
                except:
                    return {"result": "error", 'msg': 'Passing d failed'}, 403
                # if replicaDetail == "0":
                #     print("Isreplica set to FALSE")
                #     sys.stdout.flush()
                #     isReplica = False
                # elif replicaDetail == "1":
                #     print("Isreplica set to TRUE")
                #     sys.stdout.flush()
                #     isReplica = True
                # else:
                #     return {"result": "error", 'msg': 'Incorrect ID in setIsReplica'}, 403
                return {"result": "success"}, 200

            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val'].encode('ascii', 'ignore')
            except:
                value = ''
            if not value:
                return {'result': 'error', 'msg': 'No value provided'}, 403
            try:
                causalPayload = int(request.form['causal_payload'].encode('ascii', 'ignore'))
            except:
                causalPayload = ''
            try:
                key = key.encode('ascii', 'ignore')
            except:
                print("Could not encode key.")
                sys.stdout.flush()
            #Restricts key length to 1<=key<=200 characters.
            if not 1 <= len(str(key)) <= 200:
                return {'result': 'error', 'msg': 'Key not valid'}, 403
            #Restricts key to alphanumeric - both uppercase and lowercase, 0-9, and _
            if not re.match(r'^\w+$', key):
                return {'result': 'error', 'msg': 'Key not valid'}, 403

            #Restricts value to a maximum of 1Mbyte.
            if sys.getsizeof(value) > 1000000:
                return {'result': 'error', 'msg': 'Object too large. Size limit is 1MB'}, 403

            try:
                bc = int(request.form['bc'].encode('ascii', 'ignore'))
            except:
                bc = 0
            print("!!!!!!!!!BC = " + str(bc))
            sys.stdout.flush()
            if bc != 1:
                clientRequest = True
                print("!!!!!!!!!BC = " + str(bc))
                sys.stdout.flush()
            else:
                clientRequest = False
            #clientRequest = False
            #Get attached timestamp, or set it if empty.

            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
            if timestamp is '':
                tempTime = datetime.datetime.now()
                timestamp = (tempTime-datetime.datetime(1970,1,1)).total_seconds()
                #clientRequest = True

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
                else:
                    d[key] = value
                #Handle early put requests.
                if causalPayload > vClock[key]:
                    vClock[key] = causalPayload
                    storedTimeStamp[key] = timestamp

                if clientRequest == True:
                    #Increment vector clock when client put operation succeeds.
                    vClock[key] += 1
                    storedTimeStamp[key] = timestamp
                    broadcastKey(key, value, vClock[key], storedTimeStamp[key])
                return {'result': 'success', 'partition_id': partitionID, 'causal_payload': vClock[key], 'timestamp': storedTimeStamp[key]}, responseCode
            #If key already exists, set replaced to true.
            if storedTimeStamp[key] == 0:
                storedTimeStamp[key] = timestamp
            return {'result': 'success', 'partition_id': partitionID, 'causal_payload': vClock[key], 'timestamp': storedTimeStamp[key]}, responseCode

    else:
        #Handle requests from forwarding instance.
        def get(self, key):
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                answer = "No"
                if isReplica == True:
                    answer = "Yes"
                return {"result": "success", "replica": answer}, 200
            #Special command: Returns list of replicas.
            if key == 'get_all_replicas':
                return {"result": "success", "replicas": replicas}, 200

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
            while noResp:
                if len(replicas) < 1:
                    return {'result': 'error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(replicas)
                try:
                    response = requests.get(http_str + repIp + kv_str + key, data={'causal_payload': causalPayload, 'timestamp': timestamp})
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    removeReplica()
                    notInView.append(ip)
                    notInView = sortIPs(notInView)
                    continue
                noResp = False
            print(response.json())
            return response.json()

        def put(self, key):
            global replicas
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)

            if key == 'bc':
                print("SOMETHING FUCKED UP")
                sys.stdout.flush()
                return {"something fucked up":"Real good"}, 420

            #Special command: Force read repair and view update.
            if key == '_update!':
                global K, view, notInView, replicas, proxies
                try:
                    K = int(request.form['K'].encode('ascii', 'ignore'))
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    replicas = request.form['replicas'].encode('ascii', 'ignore').split(",")
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")

                except:
                    print("update failed")
                    sys.stdout.flush()
                    return {"result": "error", 'msg': 'System command parameter error'}, 403
                if proxies[0] == '':
                    proxies.pop(0)
                if replicas[0] == '':
                    replicas.pop(0)
                if notInView[0] == '':
                    notInView.pop(0)
                return {"result": "success"}, 200

            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                global isReplica
                try:
                    uni = request.form['id']
                    replicaDetail = uni.encode('ascii', 'ignore')
                except:
                    print("'ID not provided in setIsReplica")
                    sys.stdout.flush()
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403
                setReplicaDetail(uni)
                if uni == 1:
                    try:
                        d = json.loads(request.form['d'])
                        print(d)
                        sys.stdout.flush()
                    except:
                        print("request d did not work")
                        sys.stdout.flush()
                # if replicaDetail == "0":
                #     print("Isreplica set to FALSE")
                #     sys.stdout.flush()
                #     isReplica = False
                # elif replicaDetail == "1":
                #     print("Isreplica set to TRUE")
                #     sys.stdout.flush()
                #     isReplica = True
                # else:
                #     print("Incorrect ID in setIsReplic")
                #     sys.stduout.flush()
                #     return {"result": "error", 'msg': 'Incorrect ID in setIsReplica'}, 403
                return {"result": "success"}, 200

            #Makes sure a value was actually supplied in the PUT.
            try:
                uT = request.form['timestamp']
                timestamp = uT.encode('ascii', 'ignore')
            except:
                timestamp = ''
            try:
                uV = request.form['val']
                value = uV.encode('ascii', 'ignore')
            except:
                return {"result": "error", 'msg': 'No value provided'}, 403
            try:
                uC = request.form['causal_payload']
                causalPayload = uC.encode('ascii', 'ignore')
            except:
                causalPayload = ''
            try:
                key = key.encode('ascii', 'ignore')
            except:
                print("Key not encoding")
                sys.stdout.flush()
                pass
            #Try requesting random replicas
            noResp = True
            while noResp:
                if len(replicas) < 1:
                    return {'result': 'error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(replicas)
                try:
                    response = requests.put((http_str + repIp + kv_str + key), data = {'val': value, 'causal_payload': causalPayload })
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    removeReplica(repIp)
                    notInView.append(ip)
                    notInView = sortIPs(notInView)
                    continue
                noResp = False
            #Try requesting primary.
            print(response.json())
            return response.json()

        def delete(self, key):
            #Try requesting primary.
            try:
                response = requests.delete(http_str + mainAddr + kv_str + key)
            except requests.exceptions.RequestException as exc: #Handle primary failure upon delete request.
                return {'result': 'error', 'msg': 'Server unavailable'}, 500
            return response.json()

api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
    localAddress = IpPort.split(":")
    # heartBeat()
    app.run(host=localAddress[0], port=localAddress[1], threaded=True)