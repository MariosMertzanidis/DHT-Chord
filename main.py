from flask import Flask, request
import myInfo as inf
import requests as req
import checkRange as cr
import logging
import multiprocessing as mp
import sys
import json

def on_success(r):
    if r.status_code == 200:
        sys.exit()
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

app = Flask(__name__)

@app.route('/query',methods = ['POST','GET'])
def query():
    if request.method == "GET":
        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange) or receivedKey == "*":
            if (receivedHash in data) and (receivedKey in data[receivedHash]) and receivedKey != "*":

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/query'],
                                 {"data":{"key": receivedKey, "data": data[receivedHash][receivedKey]}})

                return {"respons": 'Node Contacted'}

            elif receivedKey == "*":

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/query'],
                                 {"data":{"key": receivedKey, "data": data}})

                if nextIP != receivedIP:

                    pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/query'],
                                     {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

                return {"respons": 'Node Contacted'}


            else:

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/query'],
                                 {"data":{"key": receivedKey, "data": "Error 404 - File not Found"}})

                return {"respons": 'Node Contacted'}

        else:

            pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/query'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

            return {"respons":"Message Forwarded"}

    elif request.method == 'POST':

        dataReceived = request.form.to_dict()
        if dataReceived["key"]!= "*":
            print("\nFor Key = "+dataReceived["key"]+": "+dataReceived["data"])
        else:
            print("")
            for i in dataReceived["data"]:
                for j in dataReceived["data"][i]:
                    print("For Key = " + j + ": " + dataReceived["data"][i][j])

        return {"respons":"Message Delivered"}


@app.route('/insert',methods = ['POST', 'PATCH'])
def insert():
    if request.method == "POST":

        receivedData = request.form.to_dict()["data"]

        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange):
            if receivedHash in data:
                data[receivedHash][receivedKey] = receivedData
            else:
                data[receivedHash]={}
                data[receivedHash][receivedKey] = receivedData

            pool.apply_async(req.patch,["http://"+receivedIP + ":" + receivedPort + '/insert'],
                             {"data":{"key": receivedKey, "data": "Data sucessfully installed"}})

            return {"respons": 'Node Contacted'}

        else:
            pool.apply_async(req.post,["http://"+nextIP + ":" + nextPort + '/insert'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}, "data":{"data":receivedData}})

            return {"respons": "Message Forwarded"}

    elif request.method == 'PATCH':

        dataReceived = request.form.to_dict()
        print("For Key = " + dataReceived["key"] + ":\n" + dataReceived["data"])

        return {"respons": "Message Delivered"}


@app.route('/delete',methods = ['DELETE', 'PATCH'])
def delete():
    if request.method == "DELETE":

        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange):

            del data[receivedHash][receivedKey]

            pool.apply_async(req.patch, ["http://"+receivedIP + ":" + receivedPort + '/delete'], {"data":{"key": receivedKey, "data": "Data sucessfully deleted"}})

            return {"respons": 'Node Contacted'}

        else:
            pool.apply_async(req.delete,["http://"+nextIP + ":" + nextPort + '/delete'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

            return {"respons": "Message Forwarded"}

    elif request.method == 'PATCH':

        dataReceived = request.form.to_dict()
        print("For Key = " + dataReceived["key"] + ":\n" + dataReceived["data"])

        return {"respons": "Message Delivered"}


@app.route('/remove',methods = ['POST'])
def remove():

    pool.apply_async(req.post,["http://"+prevIP + ":" + prevPort + '/changeNext'], {"data" : {"ip": nextIP, "port": nextPort}})

    pool.apply_async(req.post,["http://"+nextIP + ":" + nextPort + '/changePrev'], {"data":{"ip": prevIP, "port": prevPort, "data": data}}, callback=on_success)

    return {"respons":"Nodes Contacted"}

@app.route('/changePrev', methods=['POST'])
def changePrev():
    global myRange
    global prevIP
    global prevPort
    global data

    receivedData = request.form.to_dict()

    myRange = int(receivedData["range"])
    prevIP = receivedData["ip"]
    prevPort = receivedData["port"]

    for i in receivedData["data"]:
        if i not in data:
            data[i]={}
        for j in receivedData["data"][i]:
            data[i][j] = receivedData["data"][i][j]

    print("My new prev " + prevIP + ":" + prevPort)
    print("My new range " + str(myRange))

    return {"respons":"Data Received"}



@app.route('/newNode',methods = ['POST', 'GET'])
def newNode():

    global myRange
    global prevIP
    global prevPort
    global nextIP
    global nextPort
    global data

    if request.method == "GET":

        receivedHash = request.args.get("hash")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange):

            myNewRange = (int(receivedHash)+1)%(inf.numberOfNodes)
            dataToSend={}

            if myNewRange > myRange:
                for i in range(myRange,myNewRange):
                    if str(i) in data:
                        dataToSend[str(i)]=data[str(i)]
                        del data[str(i)]
            else:
                for i in range(myNewRange):
                    if str(i) in data:
                        dataToSend[str(i)]=data[str(i)]
                        del data[str(i)]
                for i in range(myRange,inf.numberOfNodes):
                    if str(i) in data:
                        dataToSend[str(i)]=data[str(i)]
                        del data[str(i)]

            print("Data to send: ")
            print(dataToSend)

            pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/newNode'],
                             {"data":{"nextIP": inf.myPrivateIP, "nextPort": inf.myPort, "prevIP": prevIP, "prevPort": prevPort,
                            "range": myRange, "data": json.dumps(dataToSend)}})

            pool.apply_async(req.post,["http://"+prevIP + ":" + prevPort + '/changeNext'],
                                {"data":{"ip": receivedIP, "port": receivedPort}})

            myRange = myNewRange
            prevIP =  receivedIP
            prevPort = receivedPort

            return {"respons": "Node Contacted"}

        else:
            pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/newNode'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash}})

            return {"respons":"Message Forwarded"}

    elif request.method == "POST":
        paramsReceived = request.form.to_dict()

        myRange = int(paramsReceived["range"])
        prevIP = paramsReceived["prevIP"]
        prevPort = paramsReceived["prevPort"]
        nextIP = paramsReceived["nextIP"]
        nextPort = paramsReceived["nextPort"]
        tempData = paramsReceived["data"]
        print(tempData)
        data = json.loads(tempData)
        print(data)

        print("My new next " + nextIP + ":" + nextPort)
        print("My new prev " + prevIP + ":" + prevPort)
        print("My new range " + str(myRange))

        return {"respons":"Variables have been Updated"}

@app.route('/changeNext',methods = ['POST'])
def changeNext():

    global nextIP
    global nextPort

    paramsReceived = request.form.to_dict()

    nextIP = paramsReceived["ip"]
    nextPort = paramsReceived["port"]

    print("My new next " + nextIP + ":" + nextPort)

    return {"respons":"Next has been Changed"}

if __name__ == '__main__':
    pool = mp.Pool(4)
    app.run(host="0.0.0.0", port=inf.myPort,debug = True)