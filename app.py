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
debug = False
showHeartbeat = True
exitapp = False
firstHeartBeat = True

def _print(text):
    if debug == True:
        print (text)
        sys.stdout.flush()

# Get expected environment variables.
IpPort = os.environ.get('IPPORT')
try:
    K = int(os.environ.get('K'))
except:
    K = ''
try:
    EnvString = os.environ.get('VIEW')
    EnvView = EnvString.split(',')
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

# Dictionary where key->value pairs will be stored.
d = {}
# Arrays to store replicas and proxies.
view = []
notInView = [] # Keep track of nodes not in view to see if they're back online.
replicas = []
proxies = []


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

_print(EnvView.index(IpPort))
_print(K)

# Initialize view array based on Environment Variable 'VIEW'
if EnvView is not None:
    notInView = EnvString.split(",")
    notInView = sortIPs(notInView)
if IpPort in notInView:
    notInView.remove(IpPort)
view.append(IpPort)
if ((EnvView.index(IpPort) + 1) > K ):
    proxies.append(IpPort)
    isReplica = False
else:
    replicas.append(IpPort)

def removeReplica(ip):
    replicas.remove(ip)
    view.remove(ip)

def removeProxie(ip):
    proxies.remove(ip)
    view.remove(ip)

def heartBeat():
    heart = threading.Timer(3.0, heartBeat)
    heart.daemon = True
    heart.start()
    global firstHeartBeat, replicas, proxies, view, notInView, showHeartbeat
    if firstHeartBeat:
        firstHeartBeat = False
        return

    if showHeartbeat:
        print("View: " + str(view))
        print("Replicas: " + str(replicas))
        print("Proxies: " + str(proxies))
        sys.stdout.flush()

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
    updateRatio()

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
                    pass

    if _type == 'add':
        # Check if IP is already in our view
        if ip_payload in view:
            try:
                rJ = requests.get(http_str + ip_payload + kv_str + 'get_all_replicas')
                response = rJ.json()
            except:
                _print("Node not responding")
                return {'result': 'error, node not responding on add'}, 500
            try:
                numRep = len(response['replicas'])
                _print(str(response['replicas']) + " length = " + str(numRep))
            except:
                numRep = 0

            _print ("numrep: " + str(numRep))
           
            # if numrep is 0, that means this is  a new node, but is already inside this nodes view array from an old node
            # so we have to pass it the view, K, replicas, proxies, arrays
            # if numRep > 0:
            return {'result': 'success', 'node_id': str(ip_payload), 'number_of_nodes': str(len(view))}, 200
            
        elif ip_payload != IpPort:
            view.append(ip_payload)
            view = sortIPs(view)
            if view.index(ip_payload) < K:
                # Creates new replica
                requests.put(http_str + ip_payload + kv_str + '_setIsReplica!', data={'id': 1}) #tell node to be replica
                replicas.append(ip_payload)
                replicas = sortIPs(replicas)
                _print("New replica created.")
                    
            else:
                # Creates new proxy
                requests.put(http_str + ip_payload + kv_str + '_setIsReplica!', data={'id': 0})
                proxies.append(ip_payload)
                proxies = sortIPs(proxies)
                _print("New proxie created.")
                    
        if ip_payload in notInView:
            notInView.remove(ip_payload)
        updateRatio()
        # global IpPort
        if ip_payload != IpPort:
            # global K, view, notInView, replicas, proxies
            requests.put(http_str + ip_payload + kv_str + '_update!', data = {"K": K, "view": ','.join(view), "notInView": ','.join(notInView), "replicas": ','.join(replicas), "proxies": ','.join(proxies)})
        return {"msg": "success", "node_id": ip_payload, "number_of_nodes": len(view)}, 200

    if _type == 'remove':
        # Check to see if IP is in our view
        if ip_payload not in view:
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

def updateRatio():
    global view, replicas, proxies, isReplica, K
    view = sortIPs(view)
    _print("K at start of updateRatio(): " + str(K))
    _print("view at the start of updateRatio(): " + str(view))
    _print("proxies at the start of updateRatio(): " + str(proxies))
    _print("replicas at the start of updateRatio(): " + str(replicas))

    for node in view:
        # This is a replica.
        if view.index(node) < int(K):
            _print(str(node) + " is in 'if' of updateRatio()")
            if node not in replicas:
                if node in proxies:
                    proxies.remove(node)
                replicas.append(node)
                replicas = sortIPs(replicas)
                if node == IpPort:
                    isReplica = True
                    for key in d:
                        readRepair(key)
               
        # This is a proxy.
        else:
            _print(str(node) + " is in 'else' of updateRatio()")
            if node not in proxies:
                if node in replicas:
                    replicas.remove(node)
                proxies.append(node)
                proxies = sortIPs(proxies)
                if node == IpPort:
                    isReplica = False
    
    _print("view at the end of updateRatio(): " + str(view))
    _print("proxies at the end of updateRatio(): " + str(proxies))
    _print("replicas at the end of updateRatio(): " + str(replicas))

#read-repair function
def readRepair(key):
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
            _print("Address: " + str(address)+ " Address type: " + str(type(address)))
            _print("IpPort: " + str(IpPort)+ " IpPort type: " + str(type(IpPort)))
            _print("KEY: " + str(key)+ " Address type: " + str(type(key)))
            _print("value: " + str(value)+ " value type: " + str(type(value)))
            _print("payload: " + str(payload)+ " payload type: " + str(type(payload)))
            _print("time: " + str(time)+ " time type: " + str(type(time)))
            try:
                _print("Sending to " + str(address))
                requests.put((http_str + address + kv_str + key), data = {'val': value, 'causal_payload': payload, 'timestamp': time})
            except :
                _print("broadcast failed to " + str(address))

class Handle(Resource):
    if isReplica:
        #Handles GET request
        def get(self, key):
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                answer = "No"
                if isReplica == True:
                    answer = "Yes"
                return {"result": "success", "replica": answer}, 200
            #return view of node for debugging
            if key == '_showView!':
                global view
                return{"result": "success", "view": view}, 200
            if key == '_showNotInView!':
                global notInView
                return{"result": "success", "notInView": notInView}, 200
            if key == '_showProxies!':
                global proxies
                return{"result": "success", "proxies": proxies}, 200
            #Special command: Returns list of replicas.
            if key == 'get_all_replicas':
                return {"result": "success", "replicas": replicas}, 200

            #If key is not in dict, return error.
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
            return {'result': 'success', 'value': d[key], 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': timestamp}, 200

        #Handles PUT request
        def put(self, key):
            global K, view, notInView, replicas, proxies, isReplica
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)

            #Special command: Force read repair and view update.
            if key == '_update!':
                _print("beginning of _update!")
                try:
                    K = int(request.form['K'].encode('ascii', 'ignore'))
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    replicas = request.form['replicas'].encode('ascii', 'ignore').split(",")
                except:
                    return {"result": "error", 'msg': 'System command parameter error'}, 401

                try:
                    _print(K)
                    _print(view)
                    _print(replicas)
                    _print(proxies)
                    _print(notInView)
                except:
                    return {"result": "error", 'msg': 'System command parameter error'}, 401
            
                if isReplica:
                    for key in d:
                        readRepair(key)
                
                if proxies[0] == '':
                    proxies.pop(0)
                if replicas[0] == '':
                    replicas.pop(0)
                if notInView[0] == '':
                    notInView.pop(0)
                _print("end of _update!")
                updateRatio()
                return {"result": "success"}, 200
                
            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                try:
                    uni = request.form['id']
                    replicaDetail = uni.encode('ascii', 'ignore')
                except:
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403
                if replicaDetail == "0":
                    isReplica = False
                elif replicaDetail == "1":
                    isReplica = True
                else:
                    return {"result": "error", 'msg': 'Incorrect ID in setIsReplica'}, 403
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
                _print("Could not encode key.")
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
                return {'result': 'success', 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': storedTimeStamp[key]}, responseCode
            #If key already exists, set replaced to true.
            if storedTimeStamp[key] == 0:
                storedTimeStamp[key] = timestamp
            return {'result': 'success', 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': storedTimeStamp[key]}, responseCode
            
        #Handles DEL request
        def delete(self, key):
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'error', 'msg': 'Key does not exist'}, 404

            #If key is in dict, delete key->value pair.
            del d[key]
            return {'result': 'Success', 'node_id': IP, 'causal_payload': vClock[key], 'timestamp': timestamp}, 200
        
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
            #return view,notInView and Proxies of node for debugging
            if key == '_showView!':
                global view
                return{"result": "success", "view": view}, 200
            if key == '_showNotInView!':
                global notInView
                return{"result": "success", "notInView": notInView}, 200
            if key == '_showProxies!':
                global proxies
                return{"result": "success", "proxies": proxies}, 200
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
            _print(response.json())
            return response.json()

        def put(self, key):
            global replicas
            #Special command: Force read repair and view update.
            if key == '_update!':
                global K, view, notInView, replicas, proxies
                try:
                    K = request.form['K'].encode('ascii', 'ignore')
                    view = request.form['view'].encode('ascii', 'ignore').split(",")
                    notInView = request.form['notInView'].encode('ascii', 'ignore').split(",")
                    replicas = request.form['replicas'].encode('ascii', 'ignore').split(",")
                    proxies = request.form['proxies'].encode('ascii', 'ignore').split(",")

                except:
                    _print("update failed")
                    return {"result": "error", 'msg': 'System command parameter error'}, 403
                if proxies[0] == '':
                    proxies.pop(0)
                if replicas[0] == '':
                    replicas.pop(0)
                if notInView[0] == '':
                    notInView.pop(0)
                updateRatio()
                _print("Update Ratio called in _update!")
                return {"result": "success"}, 200

            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                return updateView(self, key)

            #Special command: Force set a node's identity as replica/proxy.
            if key == '_setIsReplica!':
                global isReplica
                try:
                    uni = request.form['id']
                    replicaDetail = uni.encode('ascii', 'ignore')
                except:
                    _print("'ID not provided in setIsReplica")
                    return {"result": "error", 'msg': 'ID not provided in setIsReplica'}, 403
                if replicaDetail == "0":
                    isReplica = False
                elif replicaDetail == "1":
                    isReplica = True
                else:
                    _print("Incorrect ID in setIsReplic")
                    return {"result": "error", 'msg': 'Incorrect ID in setIsReplica'}, 403
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
                _print("Key not encoding")
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
            _print(response.json())
            return response.json()

        # def delete(self, key):
        #     #Try requesting primary.
        #     try:
        #         response = requests.delete(http_str + mainAddr + kv_str + key)
        #     except requests.exceptions.RequestException as exc: #Handle primary failure upon delete request.
        #         return {'result': 'error', 'msg': 'Server unavailable'}, 500
        #     return response.json()

api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
    localAddress = IpPort.split(":")
    heartBeat()
    app.run(host=localAddress[0], port=localAddress[1])