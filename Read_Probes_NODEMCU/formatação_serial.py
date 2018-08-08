import json
import math
inputArchvName = 'coletor_2_praca.txt'
outputArchvName = 'coleta_2_praca_Nova_formatação.txt'
shoppingName = 'Patteo_Olinda_shopping'
insideRegionShopping = 'praca_de_alimentacao'
cityPlace = 'Olinda'
arq = open(inputArchvName,'r')
listProb = arq.read().split('\n')

timeStamp = 0
arqOutput = open(outputArchvName,'w')
for i in listProb:

    if '@' not in i and '$' not in i:
        continue
    
    if '@' in i:
        x = i.find('@')
        y = i.find('@', -1)
        strTringTimeStamp = i[x+1:y]
        listTimeStamp = strTringTimeStamp.split(' ')
        timeStamp = float(listTimeStamp[1])
        print(timeStamp)
        
    if '$' in i:
        error = False
        i = i.replace('$','')
        listMacData = i.split('#')
        dicMac = {}
        if len(listMacData) < 3:
            continue
#adicionar chaves no inicio e final da string dar dumbs
#se der erro  alinha está mal formatada, depois checar
#as chaves se existem ou se estão vazias, possivelmente
#será mais rapido do que esse codigo abaixo
        for data in listMacData :
            dataList = data.split(' ')
            if dataList[0] == 'Time:':
                millis = int(dataList[1])
                sec = millis / 1000
                realTimeStamp = timeStamp + sec
                dicMac['timeStamp'] = str(math.ceil(float(realTimeStamp)))
                
            elif dataList[0] == 'RSSI:':
                dicMac['RSSI'] = dataList[1]
                
            elif dataList[0] == 'Peer':
                try:
                    dicMac['Peer MAC'] = dataList[2]
                except:
                    error = True
                    print(listMacData)
        if error:
            dicMac =={}
            error = False
        dicMac['shoppingName'] = shoppingName
        dicMac['regionShoppingName'] = insideRegionShopping
        dicMac['city'] = cityPlace 
        x = json.dumps(dicMac)
        arqOutput.write(x+'\n')
        
    
    '''if i != '':
        listaStamp = i.split('$')
        for i in listaStamp:
            if i != '':
                print(i)'''
