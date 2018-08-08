# This Python file uses the following encoding: utf-8
#!/usr/bin/env python
import json
import math

def checkMac(dicMac):
    try:
        #dicMac = json.loads(stringMac)
        if ((dicMac['Peer MAC'])[1])=='2' or ((dicMac['Peer MAC'])[1])=='3' or ((dicMac['Peer MAC'])[1])=='6' or ((dicMac['Peer MAC'])[1])=='7' or ((dicMac['Peer MAC'])[1])=='a' or ((dicMac['Peer MAC'])[1])=='b' or ((dicMac['Peer MAC'])[1])=='e' or ((dicMac['Peer MAC'])[1])=='f':
            return False
        else:
            return dicMac
    except:
        return False

inputArchvName = 'coleta_2_praca_Nova_formatação.txt'
outputArchvName = 'coleta_2_praca_formatacao_time.txt'
outputArchvNameF = 'coleta_2_praca_filtrada.txt'
#shoppingName = 'Patteo_Olinda_shopping'
#insideRegionShopping = 'praca'
#cityPlace = 'Olinda'
arq = open(inputArchvName,'r')
listProb = arq.read().split('\n')
arqOutput = open(outputArchvName,'w')
arqOutputF = open(outputArchvNameF,'w')
dicOut={}
dicOut['sendTest'] = {}
dicR = dicOut['sendTest']

for i in listProb:
    try:
        dicMac = json.loads(i)
    except:
        print(i)
    timeStamp = dicMac["timeStamp"]
    try:
        dicR[timeStamp].append(dicMac)
    except:
        dicR[timeStamp] = []
        dicR[timeStamp].append(dicMac)

for k ,v in dicR.items():
    check=[]
    resultList = []
    for singleDic in v:
        try:
            mac = singleDic["Peer MAC"]
        except:
            print(singleDic)
        if mac not in check:
            if checkMac(singleDic):                
                check.append(mac)
                resultList.append(singleDic)
                y = json.dumps(singleDic)
                arqOutputF.write(y+'\n')
    dicR[k] = resultList


    

x = json.dumps(dicOut)
arqOutput.write(x)
arqOutput.close()
arqOutputF.close()
        
