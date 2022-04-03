# -*- coding: utf-8 -*-
"""Module to test out logging to kafka."""

import json
import logging

from kafkahandler import KafkaHandler
from kafka import KafkaProducer


def run_it(logger=None):
    """Run the actual connections."""

    logger = logging.getLogger(__name__)
    # enable the debug logger if you want to see ALL of the lines
    # logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)

    kh = KafkaHandler(["172.17.13.10:9092"], "sebtest")
    logger.addHandler(kh)

    logger.info("I'm a little logger, short and stout")
    logger.debug("Don't tase me bro!")


if __name__ == "__main__":
    run_it()
