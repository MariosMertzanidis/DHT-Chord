import hashlib

mainNodeIP = "127.0.0.1"
mainNodePort = "5000"

myPrivateIP = "127.0.0.1"
myPort = "5000"

numberOfNodes = 10

numberOfReplicas = 3

nodes = [("192.168.0.3","5000"),("192.168.0.4","5000"),("192.168.0.5","5000"),("192.168.0.6","5000"),("192.168.0.7","5010")]
#9,0,8,6,2

temp = myPrivateIP+":"+myPort
myHash = int(hashlib.sha1(temp.encode("UTF-8")).hexdigest(),16)%numberOfNodes



