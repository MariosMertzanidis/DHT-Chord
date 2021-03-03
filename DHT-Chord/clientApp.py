import myInfo as inf
import requests as req
import hashlib

def insert(key, data):

    #print("\nInserting Key, Data = "+key+", "+data)

    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10

    req.post("http://" + inf.mainNodeIP + ":" + inf.mainNodePort + '/insert',
                     params = {"ip": inf.myPrivateIP, "port": inf.mainNodePort, "hash": hash, "key": key},
                      data = {"data": data})


def request(key):

    #print("\nRequesting Key = " + key)

    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10

    req.post("http://" + inf.mainNodeIP + ":" + inf.mainNodePort + '/request',
             params={"ip": inf.myPrivateIP, "port": inf.mainNodePort, "hash": hash, "key": key})


def delete(key):

    #print("\nDeleting Key = " + key)
    hash = int(hashlib.sha1(key.encode("UTF-8")).hexdigest(), 16) % 10

    req.delete("http://" + inf.mainNodeIP + ":" + inf.mainNodePort + '/delete',
             params={"ip": inf.myPrivateIP, "port": inf.mainNodePort, "hash": hash, "key": key})


def removeNode():

    #print("\nRemoving Node")

    req.post("http://" + inf.myPrivateIP + ":" + inf.myPort + '/delete')


def insertFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            key, data = line.split(", ")
            insert(key,data)


def queryFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            request(line[:-1])

def requestFromFile(file):

    with open(file, "r") as f:
        for line in f.readlines():
            args = line.split(", ")
            if len(args) == 3:
                insert(args[1],args[2])
            elif len(args) == 2:
                request(args[1])