# import os
# import numpy as np
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils

# class Markov():
# 
#     def qtd_de_pessoas(self, lista_pessoa):
# 
#         total = len(lista_pessoa)
# 
#         return total
# 
#     def localizacao_pessoa(self, lista_pessoa):
# 
#         localizacao = {}
#         localizacao['local_1'] = []
#         localizacao['local_2'] = []
#         localizacao['local_3'] = []
#         for pessoa in lista_pessoa:
#             if pessoa['local'] == 'local_1':
#                 localizacao['local_1'].append(pessoa)
#             elif pessoa['local'] == 'local_2':
#                 localizacao['local_2'].append(pessoa)
#             elif pessoa['local'] == 'local_3':
#                 localizacao['local_3'].append(pessoa)
# 
#         return localizacao
# 
#     def qtd_de_pessoas_em_cada_local(self, dicionario_locais_pessoas):
# 
#         qtd_pessoas_local = {}
#         local_1 = len(dicionario_locais_pessoas['local_1'])
#         qtd_pessoas_local['local_1'] = local_1
#         local_2 = len(dicionario_locais_pessoas['local_2'])
#         qtd_pessoas_local['local_2'] = local_2
#         local_3 = len(dicionario_locais_pessoas['local_3'])
#         qtd_pessoas_local['local_3'] = local_3
# 
#         return qtd_pessoas_local
# 
#     def calcular_total_de_pessoas_por_lugar_porcentagem(self, total_de_pessoas, qtd_pessoas_local):
# 
#         a11 = (qtd_pessoas_local['local_1'] / float(total_de_pessoas)) * 100
#         a12 = (qtd_pessoas_local['local_2'] / float(total_de_pessoas)) * 100
#         a13 = (qtd_pessoas_local['local_3'] / float(total_de_pessoas)) * 100
# 
#         matriz = np.matrix([
#             [a11, a12, a13],
#         ])
# 
#         return matriz
# 
# 
# arq = open('imagem.txt', 'r')
# x = arq.read()
# listaImagem = json.loads(x)
# 
# print(listaImagem)
# 
# m = Markov()
# 
# total_de_pessoas_3_locais = m.qtd_de_pessoas(listaImagem)
# print(total_de_pessoas_3_locais)
# 
# localizacao_pessoas_3_locais = m.localizacao_pessoa(listaImagem)
# print(localizacao_pessoas_3_locais)
# 
# total_de_pessoas_em_cada_local = m.qtd_de_pessoas_em_cada_local(localizacao_pessoas_3_locais)
# print(total_de_pessoas_em_cada_local)
# 
# array_atual = m.calcular_total_de_pessoas_por_lugar_porcentagem(total_de_pessoas_3_locais,
#                                                                 total_de_pessoas_em_cada_local)
# print(array_atual)


#ALGORITMO SPARK
if __name__ == "__main__":

    sc = SparkContext()
    ssc = StreamingContext(sc, 60)

    brokerUrl = "tcp://localhost:1883"
    topic1 = "hall"
    topic2 = "praca"

    lines_hall = MQTTUtils.createStream(ssc, brokerUrl, topic1)
#     lines_hall.pprint(100)
    
    # Split each line into macs
#     macs = lines_hall.flatMap(lambda line: line.split(",")[1::3])
#     macs.pprint(100)
    
#     dic_hall = lines_hall.flatMap(lambda line: line.split(","))
#     macs.pprint(100)
    dic_hall = {}
    keyPadrao = 'geral'
#     values = lines_hall.map(lambda line: line.split(","))
    values = lines_hall.map(lambda line : [keyPadrao, [json.loads(line)]])
#         .map(lambda k, v: dic_hall.update({k: v}), keys)
    
#     junk = map(lambda k, v: dic_hall.update({k: v}), keys, values[:])
    
#     probes_hall = lines_hall.map(lambda line: line.split(","))
    values.pprint(100)
    
    # Count each word in each batch
#     pairs = probes_hall.map(lambda word: (word, 1))
    
    #macsCounts = pairs.reduceByKey(lambda x, y: x + y)
    
    macsCounts = values.reduceByKey(lambda x, y: x + y)   
    macsCounts.pprint(100)
#     
#     lista_hall = sc.union(lines_hall)
#     lista_hall.pprint()
#     lines_hall.pprint(100)
    
    #lines_praca = MQTTUtils.createStream(ssc, brokerUrl, topic2)
#     lines_praca.pprint(100)
    
#     counts = lines.flatMap(lambda line: line.split("_")) \
#         .map(lambda word: (word, 1)) \
#         .reduceByKey(lambda a, b: a+b)
#     counts.pprint()

    ssc.start()
ssc.awaitTermination()