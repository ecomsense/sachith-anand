from threading import Thread
import time


class Thread2(Thread):
    def run(self):
        print("sleeping in thread 2")
        time.sleep(1)
        print("awake in thread 2")
