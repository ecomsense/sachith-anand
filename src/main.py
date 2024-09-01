from constants import logging, O_CNFG, O_SETG, O_FUTL
from thread1 import Thread1
from thread2 import Thread2
import time


def run():
    """
    repetitive tasks that will
    run again and again
    """
    while True:
        Thread1().start()
        Thread2().start()
        time.sleep(1)


def main():
    logging.info("HAPPY TRADING")
    """
    description:
        initialize singtons here
        also declare globals if any here
    """


run()
