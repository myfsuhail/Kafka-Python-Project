from confluent_kafka import Producer
import logging

# PROPERTIES OF PRODUCER
conf = {"bootstrap.servers": "172.17.13.10:9092"}


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        logger.debug(
            """Delivery failed for Message: \n
              key : {} : \n
              Error message : {}\n""".format(
                data.key(), errmsg
            )
        )
        return
    logger.debug(
        "Message success: \n"
        "Value with Key {} \n"
        "Topic Name: {} \n"
        "Partition: [{}] at offset {}".format(
            data.key(), data.topic(), data.partition(), data.offset()
        )
    )


def sample_producer(config=conf, logger=logger):

    producer = Producer(config)

    logger.debug("Producer started")

    items = [
        "Microsoft",
        "Apple",
        "Deloitte",
        "TCS",
        "Wipro",
        "PwC",
        "Google",
        "Facebook",
        "Informatica",
        "Snowflake",
        "Honeywell",
        "Amazon",
        "Horizon",
    ]
    for item in items:
        producer.produce("first_topic", value=item, on_delivery=delivery_report)
    producer.flush()
    logger.debug("Completed")


if __name__ == "__main__":
    sample_producer()
    logger.debug("Program completed successfully")
