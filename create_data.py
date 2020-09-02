import time
import requests
import csv
import json
import array
import names
import logging
import sys
import traceback
import os
import errno
import math

from datetime import datetime

from threading import Thread

from concurrent.futures import ThreadPoolExecutor, wait, as_completed

from multiprocessing import Process, RawValue, Lock

BASEURL = "http://192.168.1.16:8084"
NUMBER_THREADS = 1
NUMBER_THREADS_PAYLOADGEN = 5
SLEEP_TIME = 1
URI_LEARN = "/struct/kaml?nojson&nojsonlog"
TEST_FILE = "test.kaml"
TEST_ROUNDS = 1000000
#TEST_ROUNDS = 10
KAML_ENTRIES = 1200
PAYLOAD_LEADTIME=360
RESUME_FOLDER = ""
RESUME_INDEX = 0
RESUME_ROUND = 0
PAYLOAD_DIR = datetime.now().strftime("%Y%m%d-%H-%M-%S")
PAYLOAD_PATH="files/load/{}/".format(PAYLOAD_DIR)

logger = {}


def openfile(filename,mode="w+"):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    return open(filename, mode)


class Counter(object):
    def __init__(self, value=0):
        # RawValue because we don't need it to create a Lock:
        self.val = RawValue('i', value)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

#A - test relationships

#PEOPLE
#RELATIONS
#ATTRIBUTES

#B - test string sizes

#CHECK LOG DIR
#LARGE STRING > 8k
#CHANGE FILE SIZE TO ENSURE THEY MOVE OUT
payloadgeneratorpool = ThreadPoolExecutor(NUMBER_THREADS_PAYLOADGEN)
payloads= []
payloadspool = []
payloads_index = openfile("{}/load_name.csv".format(PAYLOAD_PATH), "w+")



#payload gennerator
def generatepayload(roundindex,group):
    generate_kaml_start = time.time()
    print("Starting Generating Payload round {}".format(roundindex))
    j = 0
    payload = "learn:\n"
    payload_names=""
    name = ""
    try:
        while j < KAML_ENTRIES:

            # name = names.get_full_name()
            name = "str{}".format(MASTER_INDEX.value())
            #payload_names+="{},{}\n".format(index,name)
            
            oneitem = (
                "    - [<proper>\"{}\"]\n"
                "        - [\"is\", \"here\"]\n"
                "        - [\"is\", \"live\"]\n"
                .format(name)
                ) 
            payload += oneitem
            j+=1
           
            MASTER_INDEX.increment()
            

        #print("payload length {}".format(len(payload)))
        
        #payloads_index.write(payload_names)

        payloads.append([roundindex,payload,str(time.time() - generate_kaml_start),name])

        print("round {}, index {}, group {}, total {}".format(roundindex, MASTER_INDEX.value(), group,len(payload),len(payloads)))

        #save file being processed
        f = openfile("{}/{}/load_{}.kaml".format(PAYLOAD_PATH,str(group).zfill(4),str(roundindex).zfill(4)), "w+")
        f.write(payload)

        return True
    except:
        print("Unexpected error:", sys.exc_info())
        traceback.print_exc()
        return

#long running thread making payloads
def payloadgenerator():
    group=0
    i = 0

    if RESUME_INDEX > 0:
        i = RESUME_ROUND
        group = math.trunc((RESUME_ROUND / 1000) + 1)

    print("Starting at round {} of {}, group {}, index {} in path {}"
            .format(
                str(i),
                str(TEST_ROUNDS),
                str(group),
                str(MASTER_INDEX.value()),
                PAYLOAD_PATH
            )
        )
    #exit()
    while i < TEST_ROUNDS:
        #split every 1000, this will make a new folder for each group
        if i % 1000 == 0:
            group+=1


        payloadspool.append(
            payloadgeneratorpool.submit(generatepayload, i, group)
        )

        i+=1



def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler("{}/load.log".format(PAYLOAD_PATH), mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    #logger.addHandler(screen_handler)
    return logger


def main():
    start_time = time.time()

    logger = setup_custom_logger('loadtest')

    logger.info("Staring")
    
    i = 0
    
    successcount = 0
    failurecount = 0
    print("Starting payload generator")
    thread = Thread(target = payloadgenerator, args=[])
    thread.start()
    thread.join()

    print("Starting payload generator started")



if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:

            RESUME_INDEX=sys.argv[1] #last number
            RESUME_FOLDER=sys.argv[2] #folder for load
            if RESUME_FOLDER != "":
                PAYLOAD_PATH="files/load/{}/".format(RESUME_FOLDER)

            RESUME_INDEX = int(RESUME_INDEX) + 1

            RESUME_ROUND = math.trunc(RESUME_INDEX / KAML_ENTRIES)

        except:
            print("Unexpected error:", sys.exc_info())
            traceback.print_exc()
            exit()

        
    MASTER_INDEX = Counter(RESUME_INDEX)


    main()
