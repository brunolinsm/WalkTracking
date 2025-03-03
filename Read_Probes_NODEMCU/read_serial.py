import serial
import time
ser = serial.Serial('COM3', 9600, timeout=0)

def creatNewTxt(arqName):
    try:
        arq = open(arqName+'.txt', 'r')
        
    except:
        arq = open(arqName+'.txt', 'w')
        arq.close()
        
    return arqName+'.txt'

def readSerial():
    
    response  = ser.read(9600)
    try:
        responseDecode = response.decode()
        return (responseDecode, 'OK')

    except UnicodeDecodeError:
        return (response, 'ERROR')
    

arqName = creatNewTxt('probListLog')
error = True
while True:
    time.sleep(1)
    if error :
        timeStamp = time.time()
        text = '@timeStamp '+str(timeStamp)+'@\n'
        arq = open(arqName, 'a')
        arq.write(text)
        arq.close()
        error = False
    else:
        response, status = readSerial()
        if status == 'OK':
            arq = open(arqName, 'a')
            text = response
            print(status)
            arq.write(text)
            arq.close()
        elif status == 'ERROR':
            print(status)
            error = True
            continue
        
    
