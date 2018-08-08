from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils

if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print >> sys.stderr, "Usage: mqtt_wordcount.py <broker url> <topic>"
#         exit(-1)

    sc = SparkContext()
    ssc = StreamingContext(sc, 10)

    brokerUrl = "tcp://localhost:1883"
    topic1 = "hall"
    topic2 = "praca"

    lines_hall = MQTTUtils.createStream(ssc, brokerUrl, topic1)
    lines_hall.pprint(100)
    
    # Split each line into macs
    macs = lines_hall.flatMap(lambda line: line.split(",")[1::3])
    
    # Count each word in each batch
    pairs = macs.map(lambda word: (word, 1))
    macsCounts = pairs.reduceByKey(lambda x, y: x + y)
    
    # Print the first ten elements of each RDD generated in this DStream to the console    
    macsCounts.pprint(100)
    
#     lista_hall = sc.union(lines_hall)
#     lista_hall.pprint()
#     lines_hall.pprint(100)
    
    lines_praca = MQTTUtils.createStream(ssc, brokerUrl, topic2)
#     lines_praca.pprint(100)
    
#     counts = lines.flatMap(lambda line: line.split("_")) \
#         .map(lambda word: (word, 1)) \
#         .reduceByKey(lambda a, b: a+b)
#     counts.pprint()

    ssc.start()
ssc.awaitTermination()