from flask import Flask, request
import myInfo as inf
import requests as req
from copy import deepcopy
import checkRange as cr
import logging
import multiprocessing as mp
import sys

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

@app.route('/request',methods = ['POST','GET'])
def request():
    if request.method == "GET":
        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange) or receivedKey == "*":
            if (receivedHash in data) and (receivedKey in data[receivedHash]) and receivedKey != "*":

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/request'],
                                 {"data":{"key": receivedKey, "data": data[receivedHash][receivedKey]}})

                return {"respons": 'Node Contacted'}

            elif receivedKey == "*":

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/request'],
                                 {"data":{"key": receivedKey, "data": data}})

                if nextIP != receivedIP:

                    pool.apply_async(req.get,["http://"+nextIP + ":" + nextPort + '/request'],
                                     {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

                return {"respons": 'Node Contacted'}


            else:

                pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/request'],
                                 {"data":{"key": receivedKey, "data": "Error 404 - File not Found"}})

                return {"respons": 'Node Contacted'}

        else:

            pool.apply_async(req.post,["http://"+nextIP + ":" + nextPort + '/request'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

            return {"respons":"Message Forwarded"}

    elif request.method == 'POST':

        dataReceived = request.json()
        if dataReceived["Key"]!= "*":
            print("\nFor Key = "+dataReceived["Key"]+": "+dataReceived["data"])
        else:
            print("")
            for i in dataReceived["data"]:
                for j in dataReceived["data"][i]:
                    print("For Key = " + j + ": " + dataReceived["data"][i][j])

        return {"respons":"Message Delivered"}


@app.route('/insert',methods = ['POST', 'PATCH'])
def insert():
    if request.method == "POST":

        receivedData = request.json()["data"]

        receivedHash = request.args.get("hash")
        receivedKey = request.args.get("key")
        receivedIP = request.args.get("ip")
        receivedPort = request.args.get("port")

        if cr.inMyRange(receivedHash, myRange):
            data[receivedHash][receivedKey] = receivedData

            pool.apply_async(req.patch,["http://"+receivedIP + ":" + receivedPort + '/insert'],
                             {"params":{"key": receivedKey, "data": "Data sucessfully installed"}})

            return {"respons": 'Node Contacted'}

        else:
            pool.apply_async(req.post,["http://"+nextIP + ":" + nextPort + '/insert'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}, "data":{"data":receivedData}})

            return {"respons": "Message Forwarded"}

    elif request.method == 'PATCH':

        dataReceived = request.json()
        print("For Key = " + dataReceived["Key"] + ":\n" + dataReceived["data"])

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

            pool.apply_async(req.delete, ["http://"+receivedIP + ":" + receivedPort + '/delete'], {"params":{"key": receivedKey, "data": "Data sucessfully deleted"}})

            return {"respons": 'Node Contacted'}

        else:
            pool.apply_async(req.post,["http://"+nextIP + ":" + nextPort + '/delete'],
                             {"params":{"ip": receivedIP, "port": receivedPort, "hash": receivedHash, "key": receivedKey}})

            return {"respons": "Message Forwarded"}

    elif request.method == 'PATCH':

        dataReceived = request.json()
        print("For Key = " + dataReceived["Key"] + ":\n" + dataReceived["data"])

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

    receivedData = request.json()

    myRange = receivedData["range"]
    prevIP = receivedData["ip"]
    prevPort = receivedData["port"]

    for i in receivedData["data"]:
        for j in receivedData["data"][i]:
            data[i][j] = receivedData["data"][i][j]

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

            myNewRange = (request.args.get("hash")+1)%(inf.numberOfNodes)
            dataToSend={}

            for i in range(myRange,myNewRange):
                dataToSend[i]=data[i]
                del data[i]

            pool.apply_async(req.post,["http://"+receivedIP + ":" + receivedPort + '/newNode'],
                             {"data":{"nextIp": inf.myPrivateIP, "nextPort": inf.myPort, "prevIp": prevIP, "prevPort": prevPort,
                            "range": myRange, "data": dataToSend}})

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
        paramsReceived = request.json()

        myRange = paramsReceived["range"]
        prevIP = paramsReceived["prevIP"]
        prevPort = paramsReceived["prevPort"]
        nextIP = paramsReceived["nextIP"]
        nextPort = paramsReceived["nextPort"]
        data = deepcopy(paramsReceived["data"])

        return {"respons":"Variables have been Updated"}

@app.route('/changeNext',methods = ['POST'])
def changeNext():

    global nextIP
    global nextPort

    paramsReceived = request.json()

    nextIP = paramsReceived["ip"]
    nextPort = paramsReceived["port"]

    return {"respons":"Next has been Changed"}

if __name__ == '__main__':
    pool = mp.Pool(10)
    app.run(port=inf.myPort,debug = True)
    if inf.myPrivateIP != inf.mainNodeIP:
        print("hello")
        pool.apply_async(req.get,["http://"+inf.mainNodeIP+":"+inf.mainNodePort+'/newNode'], {"params":{"ip":inf.myPrivateIP,"port":inf.myPort,"hash": inf.myHash}})
