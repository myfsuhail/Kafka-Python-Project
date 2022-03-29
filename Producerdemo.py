from confluent_kafka import Producer
import socket

def delivery_report(errmsg, data):
    """
    Reports the Failure or Success of a message delivery.
    Args:
        errmsg  (KafkaError): The Error that occured while message producing.
        data    (Actual message): The message that was produced.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(data.key(), errmsg))
        return
    print('Message: Value with Key {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        data.key(), data.topic(), data.partition(), data.offset()))


def sample_producer():
    #conf = {'bootstrap.servers': "192.168.251.169:9092"}
    conf = {'bootstrap.servers': "192.168.251.169:9092"}
    producer = Producer(conf)
    print("Started")
    producer.produce('second_topic', value="Amazon", on_delivery=delivery_report)
    producer.flush()

    print("Completed")