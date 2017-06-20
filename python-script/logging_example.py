# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 15:17:38 2017
this script shows example of using:

1. logging module: a flexible event logging system for applications and libraries.
2. time/datetime to time the runtime
@author: User
"""

import logging
from time import time
from datetime import timedelta

def setup_logging(logging_level=logging.WARNING):
    """
    Setup logging configuration
    logging_level: logging.INFO| logging.WARNING| logging.ERROR
    """
    logging.basicConfig(format='%(asctime)s   %(message)s', level=logging_level)
    return logging.getLogger('ProcessingDMPusingSPARK')

logger = setup_logging(logging.INFO)

if __name__ == "__main__":
    start = time()
    logger.info('total time usage: {0:0>8}'.format(timedelta(seconds = int(time() - start))))