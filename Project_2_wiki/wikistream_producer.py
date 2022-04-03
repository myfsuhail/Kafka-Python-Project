from confluent_kafka import Producer
import logging
import requests
import urllib

# PROPERTIES OF PRODUCER
conf = {"bootstrap.servers": "172.17.13.10:9092"}


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)


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
        logger.info(
            """Delivery failed for Message: \n
              key : {} : \n
              Error message : {}\n""".format(
                data.key(), errmsg
            )
        )
        return
    logger.info(
        "Message success: Value with Key {} ; Topic Name: {} ; Partition: [{}] at offset {}".format(
            data.key(), data.topic(), data.partition(), data.offset()
        )
    )


def sample_producer(config=conf, logger=logger):

    producer = Producer(config)
    logger.info("Producer started")

    # items = [
    #     "Microsoft",
    #     "Apple",
    #     "Deloitte",
    #     "TCS",
    #     "Wipro",
    #     "PwC",
    #     "Google",
    #     "Facebook",
    #     "Informatica",
    #     "Snowflake",
    #     "Honeywell",
    #     "Amazon",
    #     "Horizon",
    # ]
    # for item in items:
    #     producer.produce("first_topic", value=item, on_delivery=delivery_report)
    # producer.flush()
    # logger.info("Producer completed")

    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    data = requests.get(url).text
    print(data)


if __name__ == "__main__":
    sample_producer()
    logger.info("Program completed successfully")
