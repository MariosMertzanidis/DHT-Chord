import hashlib

mainNodeIP = "192.168.0.3"
mainNodePort = "5000"

myPrivateIP = "192.168.0.3"
myPort = "5000"

numberOfNodes = 10

numberOfReplicas = 1

temp = myPrivateIP+":"+myPort
myHash = int(hashlib.sha1(temp.encode("UTF-8")).hexdigest(),16)%numberOfNodes



