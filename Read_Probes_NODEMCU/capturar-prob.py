# This Python file uses the following encoding: utf-8
#!/usr/bin/env python
import pika
import sys

credentials = pika.PlainCredentials('psd', 'psd')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs',
                         exchange_type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='topic_logs',
                       queue=queue_name,
                       routing_key='Olinda.Patteo_Olinda_shopping.*')

print (' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print (" [x] " +str(method.routing_key)+':'+str(body.decode()))


channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
