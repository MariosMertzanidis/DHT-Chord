from flask import Flask, request
import myInfo as inf
import requests as req
import checkRange as cr
import logging
import multiprocessing as mp
import clientApp as ca
import sys
import json
import threading

def on_success(r):
    if r.status_code == 200:
        sys.exit()
    else:
        print(r.status_code)

def linear_query_response(r):
    if r.status_code == 200:
        print(r.status_code)
    else:
        print(r.status_code)

def on_error(ex):
    print(ex)

logger = logging.getLogger('werkzeug') # grabs underlying WSGI logger
handler = logging.FileHandler('test.log') # creates handler for the log file
logger.addHandler(handler) # adds handler to the werkzeug WSGI logger

nextIP = inf.myPrivateIP
nextPort = inf.myPort

prevIP = inf.myPrivateIP
prevPort = inf.myPort

data = {}
myRange = (inf.myHash+1)%(inf.numberOfNodes)

joined = False
numOfNodes = 1

#linera=0
#eventual=1
if sys.argv[1]=="lin":
    mode=0
else:
    mode=1

app = Flask(__name__)

@app.route('/myData')
def myData():
    dataToShow = {}
    dataToShow["Me"] = inf.myPrivateIP+":"+inf.myPort
    dataToShow["Next"] = nextIP+":"+nextPort
    dataToShow["Prev"] = prevIP + ":" + prevPort
    dataToShow["Hash"] = inf.myHash
    dataToShow["Range"] = myRange
    dataToShow["joined"] = joined
    dataToShow["data"] = data
    dataToShow["numOfNodes"] = numOfNodes
    return dataToShow

@app.route('/query',methods = ['POST','GET'])
def query():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    if request.method == "GET":
        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange, inf.myHash) or receivedKey == "*":
            if ((receivedHash not in data) or (receivedKey not in data[receivedHash])) and receivedKey != "*":

                pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                 {"data": {"key": receivedKey, "data": "Error 404 - File not Found"}})


            elif receivedKey == "*":

                receivedData = request.form.to_dict()
                receivedData = json.loads(receivedData["data"])
                #print(receivedData)
                for i in data:
                    for j in data[i]:
                        if i in receivedData:
                            receivedData[i][j] = data[i][j]
                        else:
                            receivedData[i]={}
                            receivedData[i][j] = data[i][j]

                if nextIP+":"+nextPort != receivedIP+":"+receivedPort:

                    pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/query'],
                                     {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey},"data":{"data":json.dumps(receivedData)}})
                else:
                    pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                     {"data": {"key": receivedKey, "data": json.dumps(receivedData)}})

                return {"respons": 'Node Contacted'}

            else:
                if mode == 0:
                    if inf.numberOfReplicas==1 or nextIP+":"+nextPort==inf.myPrivateIP+":"+inf.myPort:
                        pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                         {"data": {"key": receivedKey, "data": data[receivedHash][receivedKey]}})
                    else:
                        pool.apply_async(req.get, ["http://" + nextIP + ":" + nextPort + '/query'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                             "key": receivedKey,"replicas":inf.numberOfReplicas-1, "origin": inf.myPrivateIP+":"+inf.myPort}})
                else:
                    pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                     {"data": {"key": receivedKey, "data": data[receivedHash][receivedKey]}})


                return {"respons": 'Node Contacted'}


        else:

            if (receivedHash in data) and (receivedKey in data[receivedHash]):
                if mode==0:
                    receivedReplicas = int(request.args.get("replicas"))
                    if receivedReplicas>0:
                        receivedOrigin = request.args.get("origin")
                        if nextIP+":"+nextPort==receivedOrigin:
                            pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                             {"data": {"key": receivedKey, "data": data[receivedHash][receivedKey]}})
                            return {"respons":"Data Sent"}

                    if receivedReplicas == 1:
                        pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                         {"data": {"key": receivedKey, "data": data[receivedHash][receivedKey]}})
                    elif receivedReplicas > 1:
                        pool.apply_async(req.get, ["http://" + nextIP + ":" + nextPort + '/query'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                             "key": receivedKey,"replicas": receivedReplicas-1, "origin": receivedOrigin}})
                    else:
                        pool.apply_async(req.get, ["http://" + nextIP + ":" + nextPort + '/query'],
                                         {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                                     "key": receivedKey, "replicas": receivedReplicas - 1}})
                else:
                    pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/query'],
                                     {"data": {"key": receivedKey, "data": data[receivedHash][receivedKey]}})

                return {"respons":"Message Forwarded"}

            else:
                pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/query'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey,"replicas":-1}})

                return {"respons":"Next Contacted"}

        return {"respons": "Everything Good"}


    elif request.method == 'POST':

        dataReceived = request.form.to_dict()
        if dataReceived["key"]!= "*":
            print("For Key = "+dataReceived["key"]+": "+dataReceived["data"])
        else:
            dataToPrint = json.loads(dataReceived["data"])
            for i in dataToPrint:
                for j in dataToPrint[i]:
                    print("For Key = " + j + ": " + dataToPrint[i][j])

        return {"respons":"Message Delivered"}


@app.route('/insert',methods = ['POST', 'PATCH'])
def insert():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode
    if request.method == "POST":

        receivedData = request.form.to_dict()["data"]

        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")
        receivedReplicas = int(request.args.get("replicas"))

        if cr.inMyRange(receivedHash, myRange, inf.myHash) or receivedReplicas > 0:
            #print(1)

            if receivedHash in data:
                data[receivedHash][receivedKey] = receivedData
            else:
                data[receivedHash]={}
                data[receivedHash][receivedKey] = receivedData

            if cr.inMyRange(receivedHash, myRange, inf.myHash):
                #print(2)
                if receivedReplicas > 0:
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/insert'],
                                     {"data": {"key": receivedKey, "data": "Data sucessfully installed"}})
                    return {"respons": "Node Cotacted"}

                if ((inf.numberOfReplicas == 1 or nextIP+":"+nextPort==inf.myPrivateIP+":"+inf.myPort) and mode==0) or mode==1:
                    #print(3)
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/insert'],
                                     {"data": {"key": receivedKey, "data": "Data sucessfully installed"}})

                    if inf.numberOfReplicas != 1 and mode==1 and nextIP+":"+nextPort!=inf.myPrivateIP+":"+inf.myPort:
                        #print(4)
                        pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/insert'],
                                         {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                                     "key": receivedKey, "replicas": inf.numberOfReplicas - 1, "origin": inf.myPrivateIP+":"+inf.myPort},
                                          "data": {"data": receivedData}})

                else:
                    #print(5)
                    pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/insert'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                             "key": receivedKey, "replicas": inf.numberOfReplicas - 1, "origin": inf.myPrivateIP+":"+inf.myPort}, "data": {"data": receivedData}})
            else:
                #print(6)
                receivedOrigin = request.args.get("origin")
                if nextIP + ":" + nextPort == receivedOrigin:
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/insert'],
                                        {"data": {"key": receivedKey, "data": "Data sucessfully installed"}})
                    return {"respons": "Data Sent"}

                if (receivedReplicas == 1) and mode==0:
                    #print(7)
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/insert'],
                                     {"data": {"key": receivedKey, "data": "Data sucessfully installed"}})
                elif (receivedReplicas == 1) and mode==1:
                    #print(8)
                    return {"respons": 'Node Contacted'}

                else:
                    #print(9)
                    pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/insert'],
                                     {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                                 "key": receivedKey, "replicas": int(receivedReplicas) - 1, "origin": receivedOrigin},
                                      "data": {"data": receivedData}})

            return {"respons": 'Node Contacted'}

        else:
            #print(10)
            pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/insert'],
                             {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                         "key": receivedKey, "replicas": -1},
                              "data": {"data": receivedData}})

            return {"respons": 'Node Contacted'}


    elif request.method == 'PATCH':

        dataReceived = request.form.to_dict()
        print("For Key = " + dataReceived["key"] + ":\n" + dataReceived["data"])

        return {"respons": "Message Delivered"}


@app.route('/delete',methods = ['DELETE', 'PATCH'])
def delete():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode
    if request.method == "DELETE":

        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")
        receivedReplicas = int(request.args.get("replicas"))

        if cr.inMyRange(receivedHash, myRange, inf.myHash) or receivedReplicas > 0:

            if cr.inMyRange(receivedHash, myRange, inf.myHash) and (receivedHash not in data or receivedKey not in data[receivedHash]):
                pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/delete'],
                                 {"data": {"key": receivedKey, "data": "404 Data not found"}})

                return {"respons": "Node Contacted"}

            del data[receivedHash][receivedKey]

            if cr.inMyRange(receivedHash, myRange, inf.myHash):
                if ((inf.numberOfReplicas == 1 or nextIP+":"+nextPort==inf.myPrivateIP+":"+inf.myPort) and mode==0) or mode==1:
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/delete'],
                                     {"data": {"key": receivedKey, "data": "Data sucessfully deleted"}})

                    if mode==1 and inf.numberOfReplicas != 1 and nextIP+":"+nextPort!=inf.myPrivateIP+":"+inf.myPort:
                        pool.apply_async(req.delete, ["http://" + nextIP + ":" + nextPort + '/delete'],
                                         {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                                     "key": receivedKey, "replicas": inf.numberOfReplicas - 1, "origin": inf.myPrivateIP+":"+inf.myPort}})
                else:
                    pool.apply_async(req.delete, ["http://" + nextIP + ":" + nextPort + '/delete'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                             "key": receivedKey, "replicas": inf.numberOfReplicas - 1, "origin": inf.myPrivateIP+":"+inf.myPort}})
            else:
                receivedOrigin = request.args.get("origin")
                if nextIP + ":" + nextPort == receivedOrigin:
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/delete'],
                                        {"data": {"key": receivedKey, "data": "Data sucessfully deleted"}})
                    return {"respons": "Data Sent"}

                if (receivedReplicas == 1) and mode==0 :
                    pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/delete'],
                                     {"data": {"key": receivedKey, "data": "Data sucessfully deleted"}})
                elif (receivedReplicas == 1) and mode==1:
                    return {"respons": "Data deleted"}
                else:
                    pool.apply_async(req.delete, ["http://" + nextIP + ":" + nextPort + '/delete'],
                                     {"params": {"ip": receivedIP, "port": receivedPort, "hash": receivedHash,
                                                 "key": receivedKey, "replicas": receivedReplicas - 1, "origin": receivedOrigin}})

            return {"respons": "Message Forwarded"}

        else:
            pool.apply_async(req.delete,["http://"+nextIP + ":" + nextPort + '/delete'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey, "replicas":-1}})

            return {"respons": "Message Forwarded"}

    elif request.method == 'PATCH':

        dataReceived = request.form.to_dict()
        print("For Key = " + dataReceived["key"] + ":\n" + dataReceived["data"])

        return {"respons": "Message Delivered"}


@app.route('/numberOfNodes', methods=['GET'])
def numberOfNodes():
    global numOfNodes
    numOfNodes-=1
    return {"respons": numOfNodes+1}


@app.route('/remove', methods=['POST'])
def remove():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    pool.apply_async(req.post, ["http://"+prevIP + ":" + prevPort + '/changeNext'], {"data": {"ip": nextIP, "port": nextPort}})

    pool.apply_async(req.post, ["http://"+nextIP + ":" + nextPort + '/changePrev'], {"data": {"ip": prevIP, "port": prevPort, "range": myRange}})

    r = req.get("http://"+inf.mainNodeIP + ":" + inf.mainNodePort + "/numberOfNodes")

    num = int(r.json()["respons"])

    #print("Num",num)

    if num>inf.numberOfReplicas:
        #print(1)
        if inf.numberOfReplicas != 1:
            #print(2)
            pool.apply_async(req.get, ["http://" + prevIP + ":" + prevPort + '/getPrevData'],
                             {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1, "delete": 0},
                              "data": {"data": json.dumps({})}})
        else:
            #print(3)
            dataToAdd = {}
            dataToAdd["0"] = {}
            if inf.myHash > myRange:
                for i in range(myRange, inf.myHash + 1):
                    if str(i) in data:
                        dataToAdd["0"][str(i)] = data[str(i)]
            else:
                for i in range(inf.myHash + 1):
                    if str(i) in data:
                        dataToAdd["0"][str(i)] = data[str(i)]
                for i in range(myRange, inf.numberOfNodes):
                    if str(i) in data:
                        dataToAdd["0"][str(i)] = data[str(i)]


            pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                             {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1, "delete": 0},
                              "data": {"data": json.dumps(dataToAdd)}})
    else:
        nextIP = inf.myPrivateIP
        nextPort = inf.myPort
        prevIP = inf.myPrivateIP
        prevPort = inf.myPort
        data = {}
        myRange = (inf.myHash + 1) % (inf.numberOfNodes)
        joined = False

        print("Succesfuly Departed")


    return {"respons": "Nodes Contacted"}


@app.route('/changePrev', methods=['POST'])
def changePrev():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    receivedData = request.form.to_dict()

    myRange = int(receivedData["range"])
    prevIP = receivedData["ip"]
    prevPort = receivedData["port"]

    return {"respons":"Data Received"}



@app.route('/newNode',methods = ['POST', 'GET'])
def newNode():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    if request.method == "GET":

        numOfNodes+=1
        receivedHash = request.args.get("hash")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")
        receivedCheck = request.args.get("check")

        if inf.mainNodeIP+":"+inf.mainNodePort == inf.myPrivateIP+":"+inf.myPort:
            if numOfNodes <= inf.numberOfReplicas:
                receivedCheck = "0"
            else:
                receivedCheck = "1"


        if cr.inMyRange(receivedHash, myRange, inf.myHash):

            myNewRange = (int(receivedHash)+1)%(inf.numberOfNodes)
            dataToSend={}

            if receivedCheck=="1":
                if myNewRange > myRange:
                    for i in range(myRange,myNewRange):
                        if str(i) in data:
                            dataToSend[str(i)]=data[str(i)]
                else:
                    for i in range(myNewRange):
                        if str(i) in data:
                            dataToSend[str(i)]=data[str(i)]
                    for i in range(myRange,inf.numberOfNodes):
                        if str(i) in data:
                            dataToSend[str(i)]=data[str(i)]
            else:
                dataToSend = data


            pool.apply_async(req.post, ["http://"+receivedIP + ":" + receivedPort + '/newNode'],
                             {"data": {"nextIP": inf.myPrivateIP, "nextPort": inf.myPort, "prevIP": prevIP, "prevPort": prevPort,
                              "range": myRange, "check": receivedCheck, "data": json.dumps(dataToSend)}})

            pool.apply_async(req.post, ["http://"+prevIP + ":" + prevPort + '/changeNext'],
                             {"data": {"ip": receivedIP, "port": receivedPort}})

            myRange = myNewRange
            prevIP = receivedIP
            prevPort = receivedPort

            return {"respons": "Node Contacted"}

        else:
            pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/newNode'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "check": receivedCheck}})

            return {"respons":"Message Forwarded"}

    elif request.method == "POST":
        global joined
        joined = True
        paramsReceived = request.form.to_dict()

        myRange = int(paramsReceived["range"])
        prevIP = paramsReceived["prevIP"]
        prevPort = paramsReceived["prevPort"]
        nextIP = paramsReceived["nextIP"]
        nextPort = paramsReceived["nextPort"]
        tempData = paramsReceived["data"]
        check = paramsReceived["check"]
        data = json.loads(tempData)

        if check == "0":
            print("Succesfuly Joined")
        else:
            if inf.numberOfReplicas != 1:
                pool.apply_async(req.get, ["http://" + prevIP + ":" + prevPort + '/getPrevData'],
                                {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1,"delete": 1},"data":{"data":json.dumps({})}})
            else:
                dataToDel={}
                dataToDel["0"]={}
                dataToDel["0"]["range"]=(inf.myHash,myRange)

                pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                                 {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1, "delete": 1},
                                  "data": {"data": json.dumps(dataToDel)}})


        return {"respons":"Variables have been Updated"}

@app.route('/getPrevData',methods = ['GET','POST'])
def getPrevData():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    if request.method == 'GET':
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")
        receivedReplicas = request.args.get("replicas")
        receivedBool = request.args.get("delete")

        dataReceived = json.loads(request.form.to_dict()["data"])
        #print("Data received",dataReceived)
        dataReceived[receivedReplicas]={}
        dataReceived[receivedReplicas]["range"]=(inf.myHash,myRange)
        if inf.myHash > myRange:
            for i in range(myRange, inf.myHash+1):
                if str(i) in data:
                    dataReceived[receivedReplicas][str(i)] = data[str(i)]
        else:
            for i in range(inf.myHash+1):
                if str(i) in data:
                    dataReceived[receivedReplicas][str(i)] = data[str(i)]
            for i in range(myRange, inf.numberOfNodes):
                if str(i) in data:
                    dataReceived[receivedReplicas][str(i)] = data[str(i)]

        #print("Data forwarded", dataReceived)

        if inf.numberOfReplicas-1 == int(receivedReplicas):
            pool.apply_async(req.post, ["http://" + receivedIP + ":" + receivedPort + '/getPrevData'],
                             {"params": {"delete": receivedBool},
                              "data": {"data": json.dumps(dataReceived)}})
        else:
            pool.apply_async(req.get, ["http://" + prevIP + ":" + prevPort + '/getPrevData'],
                             {"params": {"ip": receivedIP, "port": receivedPort, "replicas": int(receivedReplicas)+1, "delete": receivedBool},
                              "data": {"data": json.dumps(dataReceived)}})

        return {"respons":"Node Contacted"}

    elif request.method == 'POST':

        receivedBool = request.args.get("delete")

        if receivedBool == "1":
            dataReceived = json.loads(request.form.to_dict()["data"])

            dataToDel = {}

            #print("Post data received", dataReceived)

            for i in dataReceived:
                dataToDel[i]={}
                for j in dataReceived[i]:
                    if j != "range":
                        if j not in data:
                            data[j]={}
                        for z in dataReceived[i][j]:
                            data[j][z]=dataReceived[i][j][z]
                    else:
                        dataToDel[i]["range"]=dataReceived[i]["range"]
            dataToDel["0"] = {}
            dataToDel["0"]["range"]=(inf.myHash,myRange)

            pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                             {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1, "delete": 1},
                              "data": {"data": json.dumps(dataToDel)}})
        else:
            dataReceived = json.loads(request.form.to_dict()["data"])
            dataReceived["0"]={}
            if inf.myHash > myRange:
                for i in range(myRange, inf.myHash + 1):
                    if str(i) in data:
                        dataReceived["0"][str(i)] = data[str(i)]
            else:
                for i in range(inf.myHash + 1):
                    if str(i) in data:
                        dataReceived["0"][str(i)] = data[str(i)]
                for i in range(myRange, inf.numberOfNodes):
                    if str(i) in data:
                        dataReceived["0"][str(i)] = data[str(i)]

            pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                             {"params": {"ip": inf.myPrivateIP, "port": inf.myPort, "replicas": 1, "delete": 0},
                              "data": {"data": json.dumps(dataReceived)}})

        return {"respons": "Data received"}

@app.route('/changeData',methods = ['POST','PATCH'])
def changeData():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    if request.method == 'POST':
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")
        receivedReplicas = request.args.get("replicas")
        receivedBool = request.args.get("delete")

        dataReceived = json.loads(request.form.to_dict()["data"])

        if receivedBool == "1":
            myID = str(inf.numberOfReplicas - int(receivedReplicas))
            receivedHash, receivedRange = dataReceived[myID]["range"]

            if int(receivedHash) >= int(receivedRange):
                for i in range(int(receivedRange),int(receivedHash)+1):
                    if str(i) in data:
                        del data[str(i)]
            else:
                for i in range(int(receivedHash)+1):
                    if str(i) in data:
                        del data[str(i)]
                for i in range(int(receivedRange),inf.numberOfNodes):
                    if str(i) in data:
                        del data[str(i)]

            del dataReceived[myID]

            if myID == "0":
                pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/changeData'],
                                 {"data": {"respons": "Succesfully Joined"}})
            else:
                pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "replicas": int(receivedReplicas)+1, "delete": 1},
                                  "data": {"data": json.dumps(dataReceived)}})

            return{"respons":"Node Contacted"}

        else:
            myID = str(inf.numberOfReplicas - int(receivedReplicas))
            dataToInsert = dataReceived[myID]
            #print(dataToInsert)
            for i in dataToInsert:
                if i != "range":
                    if i not in data:
                        data[i]={}
                    for j in dataToInsert[i]:
                        data[i][j]=dataToInsert[i][j]

            del dataReceived[myID]

            if myID == "0":
                pool.apply_async(req.patch, ["http://" + receivedIP + ":" + receivedPort + '/changeData'],
                                 {"data": {"respons": "Succesfully Departed"}})
            else:
                pool.apply_async(req.post, ["http://" + nextIP + ":" + nextPort + '/changeData'],
                                 {"params": {"ip": receivedIP, "port": receivedPort, "replicas": int(receivedReplicas)+1, "delete": 0},
                                  "data": {"data": json.dumps(dataReceived)}})

            return {"respons":"Node Contacted"}



    elif request.method == 'PATCH':

        toPrint = request.form.to_dict()["respons"]
        print(toPrint)

        if toPrint == "Succesfully Departed":
            nextIP = inf.myPrivateIP
            nextPort = inf.myPort
            prevIP = inf.myPrivateIP
            prevPort = inf.myPort
            data = {}
            myRange = (inf.myHash + 1) % (inf.numberOfNodes)
            joined = False

        return {"respons": "Succesful Delivery"}


@app.route('/changeNext',methods = ['POST'])
def changeNext():
    global nextIP
    global nextPort
    global prevIP
    global prevPort
    global data
    global myRange
    global joined
    global numOfNodes
    global mode

    paramsReceived = request.form.to_dict()

    nextIP = paramsReceived["ip"]
    nextPort = paramsReceived["port"]

    return {"respons":"Next has been Changed"}

def application():

    while True:
        userInput = input().split()

        if userInput[0] == "insert":
            if len(userInput)!=3:
                print("Invalid Syntax")
            else:
                ca.insert(userInput[1], userInput[2], inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "request":
            if len(userInput) != 2:
                print("Invalid Syntax")
            else:
                ca.request(userInput[1],inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "delete":
            if len(userInput) != 2:
                print("Invalid Syntax")
            else:
                ca.delete(userInput[1],inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "join":
            ca.join()
        elif userInput[0] == "depart":
            ca.depart()
            break
        elif userInput[0] == "insertFromFile":
            if len(userInput) != 2:
                print("Invalid Syntax")
            else:
                ca.insertFromFile(userInput[1])
        elif userInput[0] == "queryFromFile":
            if len(userInput) != 2:
                print("Invalid Syntax")
            else:
                ca.queryFromFile(userInput[1])
        elif userInput[0] == "requestFromFile":
            if len(userInput) != 2:
                print("Invalid Syntax")
            else:
                ca.requestFromFile(userInput[1])
        elif userInput[0] == "help":
            ca.help()
        else:
            print("Invalid Syntax")


if __name__ == '__main__':
    pool = mp.Pool(5)
    t1 = threading.Thread(target=application)
    t1.start()
    app.run(host="0.0.0.0",port=inf.myPort,debug = True)