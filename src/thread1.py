from threading import Thread
import time


class Thread1(Thread):
    def run(self):
        print("sleeping in thread1")
        time.sleep(1)
        print("awake in thread1")
