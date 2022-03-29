from confluent_kafka import Producer
import socket
conf = {'bootstrap.servers': "192.168.238.195:9092"}
print(1)
producer = Producer(conf)
print(2)
producer.produce('second_topic', key="key", value="value")
print(3)
producer.flush()
print(4)