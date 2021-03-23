import myInfo as inf
import requests as req
import hashlib
import json
import random
import time

def insert(key, data, ip,port):

    #print("\nInserting Key, Data = "+key+", "+data)

    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10

    req.post("http://" + ip + ":" + port + '/insert',
                     params = {"ip": inf.myPrivateIP, "port": inf.myPort, "hash": hash, "key": key, "replicas":-1},
                      data = {"data": data})


def request(key, ip,port):

    #print("\nRequesting Key = " + key)

    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10
    temp = {}
    req.get("http://" + ip + ":" + port + '/query',
             params={"ip": inf.myPrivateIP, "port": inf.myPort, "hash": hash, "key": key,"replicas":-1},data={"data": json.dumps(temp)})


def delete(key,ip,port):

    #print("\nDeleting Key = " + key)
    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10

    req.delete("http://" + ip + ":" + port + '/delete',
             params={"ip": inf.myPrivateIP, "port": inf.myPort, "hash": hash, "key": key, "replicas":-1})


def overlay():

    req.get("http://" + inf.myPrivateIP + ":" + inf.myPort + '/overlay',
             data={"ip": inf.myPrivateIP, "port": inf.myPort, "overlay": '[]'})

def depart():

    #print("\nRemoving Node")

    req.post("http://" + inf.myPrivateIP + ":" + inf.myPort + '/remove')

def join():

    #print("\nRemoving Node")

    req.get("http://"+inf.mainNodeIP+":"+inf.mainNodePort+"/newNode", params={"ip":inf.myPrivateIP,"port": inf.myPort,"hash": inf.myHash, "replicas": -1, "check": -1})

def insertFromFile(file):
    with open(file, "r") as f:
        start_time = time.time()
        for line in f.readlines():
            key, data = line[:-1].split(", ")
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            insert(key,data, randIP, randPort)
        print("Time: %.2f sec"%(time.time()-start_time))



def queryFromFile(file):
    with open(file, "r") as f:
        start_time = time.time()
        for line in f.readlines():
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            request(line[:-1], randIP, randPort)
        print("Time: %.2f sec" % (time.time() - start_time))


def requestFromFile(file):
    with open(file, "r") as f:
        start_time = time.time()
        for line in f.readlines():
            args = line[:-1].split(", ")
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            if len(args) == 3:
                insert(args[1],args[2], randIP, randPort)
            elif len(args) == 2:
                request(args[1], randIP, randPort)
        print("Time: %.2f sec" % (time.time() - start_time))

def help():
    print("Commands:\n"
          "insert <key>, <data>\n"
          "request <key>\n"
          "delete <key>\n"
          "join\n"
          "depart\n"
          "overlay\n"
          "insertFromFile <path/to/file>\n"
          "queryFromFile <path/to/file>\n"
          "requestFromFile <path/to/file>")

def application():
    global dataIsHere
    while True:
        userInput = input().split(" ",1)

        if userInput[0] == "insert":
            keyData = userInput[1].split(", ")
            if len(keyData)!=2:
                print("Invalid Syntax")
            else:
                insert(keyData[0], keyData[1], inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "request":
            request(userInput[1],inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "delete":
            delete(userInput[1],inf.myPrivateIP, inf.myPort)
        elif userInput[0] == "join":
            if len(userInput)!=1:
                print("Invalid Syntax")
            else:
                join()
        elif userInput[0] == "depart":
            if len(userInput)!=1:
                print("Invalid Syntax")
            else:
                depart()
                break
        elif userInput[0] == "insertFromFile":
            insertFromFile(userInput[1])
        elif userInput[0] == "queryFromFile":
            queryFromFile(userInput[1])
        elif userInput[0] == "requestFromFile":
            requestFromFile(userInput[1])
        elif userInput[0] == "help":
            if len(userInput)!=1:
                print("Invalid Syntax")
            else:
                help()
        elif userInput[0] == "overlay":
            if len(userInput)!=1:
                print("Invalid Syntax")
            else:
                overlay()
        else:
            print("Invalid Syntax")