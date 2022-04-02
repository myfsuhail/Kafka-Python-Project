from pydoc_data.topics import topics
from confluent_kafka import Consumer, Producer
import logging
import sys

# configure settings for consumer
conf = {
    "bootstrap.servers": "172.17.13.10:9092",
    "group.id": "mygroup",
    "session.timeout.ms": 30000,
    "auto.offset.reset": "earliest",
}


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)


def sample_consumer(config, logger=logger, max_session=10):
    consumer = Consumer(conf)
    logger.debug("Inside consumer")
    consumer.subscribe(["first_topic"])
    session = 0

    while True:
        message = consumer.poll(timeout=5)
        if message is None:
            # logger.debug("No new message received")
            # session += 1
            if session > max_session:
                logger.debug("Maximum session count exceeded")
                break
            # continue
        else:
            logger.debug(message.value().decode("utf8"))


if __name__ == "__main__":
    logger.debug("Hi")
    sample_consumer(conf)
