# This Python file uses the following encoding: utf-8
#!/usr/bin/env python
import paho.mqtt.client as paho
import time
import json
from __builtin__ import type
 
mqtthost = "localhost"  
mqttuser = "psd"  
mqttpass = "psd"  
mqtttopic = "praca"  
 
client = paho.Client()
client.username_pw_set(mqttuser,mqttpass)
client.connect(mqtthost, 1883,60)


client.loop_start()


teste = open('coleta_2_praca_formatacao_time.txt', 'r')
probList = teste.read().split('\n')
#adicionar nome do coletor e local de coleta ex: coletor_hall e shoppingPateo
#{"Peer MAC": "ac:0d:1b:e0:0e:ad", "timeStamp":1529180406.0857482,
#"RSSI": "-83"}
dic = json.loads(probList[0])
dicSendTest = dic["sendTest"]
listKey = sorted(dicSendTest)

x = 0
for timeStampKey in listKey:
    probList = dicSendTest[timeStampKey]
    for probDic in probList:
        mac = probDic["Peer MAC"]
        timeStamp = probDic["timeStamp"]
        rssi = probDic["RSSI"]
        shoppingName = probDic["shoppingName"]
        shoppingRegion = probDic["regionShoppingName"]
        city = probDic["city"]
        rotaMQTT = '{}.{}.{}'.format(city,shoppingName,shoppingRegion) #nomeDoLocal.nomeDaRegiãoDoLocal
        bodyMessage = '{},{},{}'.format(timeStamp, mac, rssi)
        (rc, mid) = client.publish(mqtttopic, bodyMessage, qos=1)
        print (" [x] Sent %r:%r" % (rotaMQTT, bodyMessage))
    print (x)
    x += 1
    if x == 10:
        x = 0
        time.sleep(10)