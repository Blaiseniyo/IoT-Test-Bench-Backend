import random
import time
from paho.mqtt import client as mqtt_client
import psutil
from random import choice
from string import ascii_uppercase

def radom_message(size):
    message = ''.join(choice(ascii_uppercase) for i in range(size))
    return message
class Broker:
    run_flag = True
    messages_received = 0
    results={"sentMessages":0,"recievedMessages":0,"sentMessages_percentage":0,"failedMessage":0,"messages_per_second":0,"totalTime":0, "sentSize":0, "recievedSize":0,"CPU_used":0,"RAM_used":0}

    def connect_mqtt(self,broker,port,username,password,num):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                return True
            else:
                return False

        def on_log(client, userdata, level, buf):
            print("log: ",buf)

        def on_message(client, userdata, message):
            self.messages_received +=1
            self.results["recievedMessages"]+=1
        
        client_id = f'client-{num}-{random.randint(0, 1000)}'
        client = mqtt_client.Client(client_id)
        client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.on_message = on_message
        # client.on_log=on_log
        try:
            client.connect(broker, port)
            return client
        except:
            print("connection failed")

    def publish(self,client,topic,size):
        msg = radom_message(size)
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            self.results["sentMessages"]+=1
        else:
            self.results["failedMessage"]+=1


    def check_connection(host,port,username, password):
        def on_connect(client, userdata, flags, rc):
            global connected
            if rc == 0:
                connected =True
            else:
                connected = False

        client = mqtt_client.Client("testUser")
        client.username_pw_set(username, password)
        client.on_connect = on_connect
        try:
            global connection_flag
            client.connect(host, port)
            connection_flag = True
            client.loop_start()
            time.sleep(1)
            return {"client": client,"connected":connected}
        except:
            return {"client": client,"connected":False}


    def run(self,body):
        #initailizing 
        program_start = time.time()
        self.results = {"sentMessages":0,"recievedMessages":0,"sentMessages_percentage":0,"failedMessage":0,"messages_per_second":0,"totalTime":0, "sentSize":0, "recievedSize":0,"CPU_used":0,"RAM_percent_used":0,"RAM_size_used":0}

        self.messages_received = 0
        clients = []
        subscribs = []
        msg_count = 0
        connected = self.check_connection(body["host"],body["port"],body["username"],body["password"])["connected"]

        if connected:
            #subscribing to the topic
            for subscriber in range(body["subscribers"]):
                client = self.connect_mqtt(self,broker=body['host'],port=body["port"],username=body["username"],password=body["password"],num=subscriber)
                client.loop_start()
                client.subscribe(body["topic"]+"/"+body["topicLevel"])
                subscribs.append(client)
        
            #publishing messages to the topic and calculate the it takes
            start_time=time.time()            
            for publisher in range(body["publishers"]):
                client = self.connect_mqtt(self,broker=body['host'],port=body["port"],username=body["username"],password=body["password"],num=publisher)
                client.loop_start()
                self.publish(self,client,body["topic"]+"/"+body["topicLevel"],body["message_size"])
                msg_count += 1
                time.sleep(body["message_interval"])
                clients.append(client)

            #calculating the results
            self.results["totalTime"] = int((time.time()-start_time))
            self.results["messages_per_second"] = int(self.results["totalTime"]/msg_count)
            self.results["recievedSize"]= self.results["recievedMessages"] * body["message_size"]
            self.results["sentSize"] = self.results["sentMessages"] * body["message_size"]
            self.results["sentMessages_percentage"] = int(((self.results["recievedMessages"]/body["subscribers"]) / self.results["sentMessages"]) * 100)
            self.results["CPU_used"] = psutil.cpu_percent(int(time.time()-program_start))
            self.results["RAM_percent_used"] = psutil.virtual_memory()[2]
            self.results["RAM_size_used"] = int(psutil.virtual_memory()[3]/1048576)
            #disconnectiing all publishers
            self.run_flag=False
            for client in clients:
                client.loop_stop()
                client.disconnect()
            time.sleep(2)
            
            #disconnectiing all subscribes
            for sub in subscribs:
                sub.loop_stop()
                sub.disconnect()

            return {"connection_status":connected,"message":"connected successfuly","data":self.results}
        else:
            return {"message":"can not connect to the broker", "connection_status":connected}