import myInfo as inf
import requests as req
import hashlib
import json
import random

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


def depart():

    #print("\nRemoving Node")

    req.post("http://" + inf.myPrivateIP + ":" + inf.myPort + '/remove')

def join():

    #print("\nRemoving Node")

    req.get("http://"+inf.mainNodeIP+":"+inf.mainNodePort+"/newNode", params={"ip":inf.myPrivateIP,"port": inf.myPort,"hash": inf.myHash, "replicas": -1, "check": -1})


def insertFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            key, data = line.split(", ")
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            insert(key,data, randIP, randPort)


def queryFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            request(line, randIP, randPort)

def requestFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            args = line.split(", ")
            randIP, randPort = inf.nodes[random.randint(0, 4)]
            if len(args) == 3:
                insert(args[1],args[2], randIP, randPort)
            elif len(args) == 2:
                request(args[1], randIP, randPort)


def help():
    print("Documentation ... ")