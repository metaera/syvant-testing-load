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
import signal
import sys
from pathlib import Path
from datetime import datetime
from threading import Thread, Event
from time import gmtime, strftime

from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from multiprocessing import Pool, Process, Lock
from queue import Queue

from blessings import Terminal

term = Terminal()

import asyncio
import websockets


BASEURL = 'http://192.168.1.16:8084'
BASEURL_WS = 'ws://192.168.1.16:8085/struct/kaml'
NUMBER_THREADS = 1
SLEEP_TIME = 1
URI_LEARN = '/struct/kaml?nojson&nojsonlog'
# URI_LEARN = '/struct/kaml?nojson'
DATA_DIR = './files/load'
RESUME_INDEX=0
RUN_ROUNDS=1

TEST_ROUNDS = 1

TIMESTAMP = datetime.now().strftime("%Y%m%d-%H-%M-%S")

logger = {}
#A - test relationships

#PEOPLE
#RELATIONS
#ATTRIBUTES

#B - test string sizes

#CHECK LOG DIR
#LARGE STRING > 8k
#CHANGE FILE SIZE TO ENSURE THEY MOVE OUT

successcount = 0
failurecount = 0 


def signal_handler(signum, frame):
    quitEvent.set()
    # print('You pressed Ctrl+C! Aborting...')
    # abortrun = True
    return
#    sys.exit(0)



def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler("files/load-{}.log".format(TIMESTAMP), mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    #logger.addHandler(screen_handler)
    return logger


def learn(kamlpath, index, logger, queentry):
    quetime = str(time.time() - queentry)
    print("Processing: {}".format(kamlpath))
    try: 
        kaml = ""

        kamlload_start = time.time()

        with open(kamlpath, 'r') as file:
            kaml = file.read()

        # add new contect command
        kaml += "    - { \"command\": \"new context\" }"

        kamlload_end = str(time.time() - kamlload_start)
        
        post_start = time.time()

        r = requests.post(BASEURL + URI_LEARN, data=kaml)

        post_end = str(time.time() - post_start)

        #logger.info(",{},{}".format(index, name))
        
        #logger.info(",{},{}".format(index, r.text))


        #code = 200
        code = r.status_code

        if code == 200:
            info = {
                'kamlpath': kamlpath,
                'index': index,
                'code': code,
                'kamlloadtime': kamlload_end,
                'posttime': post_end,
                'quetime': quetime,
            }
            return True, info
        else:
            info = {
                'kamlpath': kamlpath,
                'index': index,
                'code': code,
                'kamlloadtime': kamlload_end,
                'posttime': post_end,
                'quetime': quetime,
            }
            return False, info    
    except:
        print("Unexpected error:", sys.exc_info())
        traceback.print_exc()
        exit

    
def multi_thread():
    start_time = time.time()

    logger = setup_custom_logger('loadtest')

    logger.info("Staring")
    
    i = 0
    resumeload = False
    matchfilename = ""

    if RESUME_INDEX != "0":
        print("Resuming at {}".format(RESUME_INDEX))
        matchfilename = str(RESUME_INDEX)
        resumeload = True

    #exit()

    successcount = 0
    failurecount = 0
    
    executor = ThreadPoolExecutor(NUMBER_THREADS)
    futures = []    

    print("Start Loading Payloads")

    pathlist = Path(DATA_DIR).glob('**/*.kaml')

    for file in pathlist:
        filepath = str(file)
        if (resumeload):
            if (matchfilename in filepath):
                resumeload = False
                print("Found resume file {}".format(filepath))
                #exit()
            else:
                #Skip files until we find the one we loading first
                continue

        # result = learn( filepath, i, logger, time.time())

        # logger.info(",{},{},{},{},{},{}".format(
        #     result[1]['kamlpath'],
        #     result[1]['index'],
        #     result[1]['code'],
        #     result[1]['kamlloadtime'],
        #     result[1]['posttime'],
        #     result[1]['quetime'])
        # )
        futures.append(
            executor.submit(learn, filepath, i, logger, time.time())
        )

        i += 1    

        
        
    print("Total files found: {}".format(i))
    

    for x in as_completed(futures):
        
        logger.info(",{},{},{},{},{},{}".format(
                x.result()[1]['kamlpath'],
                x.result()[1]['index'],
                x.result()[1]['code'],
                x.result()[1]['kamlloadtime'],
                x.result()[1]['posttime'],
                x.result()[1]['quetime'])
            )

        if x.result()[0]:
            successcount += 1
        else:
            failurecount += 1
    
    
    logger.info("Success: " + str(successcount))
    logger.info("Failed: " + str(failurecount))
    logger.info("Total time: " + str(time.time() - start_time))

# async def send_data_async(index,kaml,logger)():
#     uri = "ws://localhost:8765" + URI_LEARN
#     async with websockets.connect(uri) as websocket:
#         await websocket.send(kaml)
#         await websocket.recv()

def send_data(kaml):
    try:
        post_start = time.time()

        r = requests.post(BASEURL + URI_LEARN, data=kaml)

        post_end = str(time.time() - post_start)

        #code = 200
        code = r.status_code
        # success = False
        # if code == 200:
        #     success = True
        #     #successcount += 1
        # else:
        #     #failurecount += 1
        return code
    except:
        print("Unexpected error:", sys.exc_info())
        traceback.print_exc()
        exit

    return 0


def work(lock, num, q, kaml, quit):
    try:
        time.sleep(1)
        # wait(3000)

        transactions_rate = 0
        transactions_start = 0
        last_call = 0
        interval_in_sec = 1
        i = 0
        while True:
            if quit.isSet():
                return
            if not q.empty():
                item = q.get()

                post_start = time.time()
                result = send_data(kaml)
                post_end = str(time.time() - post_start)

                logger.info(",{},{},{},{}".format(
                        num,
                        item[0],
                        result,
                        post_end
                    )
                )
                    # print('processing', item[0])

                if (time.time() - last_call) > interval_in_sec:
                    transactions_rate = i - transactions_start
                    transactions_start = i
                    # transactions_rate = i - transactions_start
                    # transactions_start = i
                    last_call = time.time()

                with term.location():
                    try:
                        lock.acquire()
                        print(term.move(num + 1, 0) + '[{}]: i={}, T/sec={}, data length={}'.format(num,item[0],transactions_rate, len(kaml)))
                    finally:
                        lock.release()
                        # print('Thread {}, Queue: {}'.format(num, q.qsize()), end='\r')
                i += 1
                q.task_done()
            #     print("Unexpected error:", sys.exc_info())
            #     traceback.print_exc()
            #     exit
    # except KeyboardInterrupt:
        # try:
            # lock.acquire()
        # with term.location(0, NUMBER_THREADS + 1):
        #     print('Interrupted!')
    except:
        print("Unexpected error:", sys.exc_info())
        traceback.print_exc()
        exit
    finally:
        return

def feedque(num, q, quit):
    
    i = 0

    while i < RUN_ROUNDS:
        if quit.isSet():
            return
        q.put([i])    
        
    #     #executor.submit(send_data, i, kaml, logger)

        i += 1

        with term.location():
            # try:
            #     lock.acquire()
            print(term.move(0, 0) + 'Current Time:\t{}, Rounds: {}, Queue: {}'.format(str(datetime.now()), f"{i:,}",  q.qsize()), end='\r')
            # finally:
            #     lock.release()


def single_thread():
    start_time = time.time()


    logger.info("Staring")
    
    print("Preparing Loading Payload")


    kamlload_start = time.time()

    with open("load_0000_1200.kaml", 'r') as file:
            kaml = file.read()

    kaml += "    - { \"command\": \"new context\" }"

    kamlload_end = str(time.time() - kamlload_start)
    print("Funning for {} rounds".format(f"{RUN_ROUNDS:,}"))
    print("Running press Ctr+C to abort.")    
    logger.info("Starting at " + str(datetime.now()))
    print("Starting at \t" + str(datetime.now()))
    try:

        #executor = ThreadPoolExecutor(NUMBER_THREADS)
        print(term.clear())

        for num in range(NUMBER_THREADS):
            if num == 0:
                #Process(target=feedque, args=()).start()    
                worker = Thread(target=feedque, args=(num, dataqueue, quitEvent), daemon=True).start()
            worker = Thread(target=work_ws, args=(lock, num, dataqueue, kaml, quitEvent ), daemon=True).start()

        dataqueue.join()

    except KeyboardInterrupt:
        quitEvent.set()
        # try:
            # lock.acquire()
        with term.location(0, NUMBER_THREADS + 10):
            print('Interrupted!')
        # finally:
        #     lock.release()


    try:
        lock.acquire()
        with term.location(0,  NUMBER_THREADS + 1):
            print('Done!')
    finally:
        lock.release()

    logger.info("End at " + str(datetime.now()))
    print("End at \t\t" + str(datetime.now()))
    
    logger.info("Success: " + str(successcount))
    logger.info("Failed: " + str(failurecount))
    logger.info("Total time: " + str(time.time() - start_time))

def single_thread2():
    start_time = time.time()

    logger = setup_custom_logger('loadtest')

    logger.info("Staring")
    
    i = 0
    print("Preparing Loading Payload")

    kaml = ""

    kamlload_start = time.time()

    with open("load_0000_1200.kaml", 'r') as file:
            kaml = file.read()

    kaml += "    - { \"command\": \"new context\" }"

    kamlload_end = str(time.time() - kamlload_start)
    print("Funning for {} rounds".format(f"{RUN_ROUNDS:,}"))
    print("Running press Ctr+C to abort.")    
    logger.info("Starting at " + str(datetime.now()))
    print("Starting at \t" + str(datetime.now()))
    transactions_rate = 0
    transactions_start = 0
    last_call = 0
    interval_in_sec = 1
    try:

        #executor = ThreadPoolExecutor(NUMBER_THREADS)
        # q = Queue()


        # for num in range(NUMBER_THREADS):
        #     Process(target=work, args=(lock, num, q)).start()

        while True:
            # q.put([i, kaml, logger])    
            
            #executor.submit(send_data, i, kaml, logger)
            send_data(i,kaml, logger)

            i += 1

            if (time.time() - last_call) > interval_in_sec:
                # transactions_rate = successcount - transactions_start
                # transactions_start = successcount
                transactions_rate = i - transactions_start
                transactions_start = i
                last_call = time.time()

            print('Current Time:\t{}, Rounds: {}, Tx/S: {}, Queue: {}'.format(str(datetime.now()), f"{i:,}", transactions_rate, 0), end='\r')

    except KeyboardInterrupt:
        print('Interrupted!')

    logger.info("End at " + str(datetime.now()))
    print("End at \t\t" + str(datetime.now()))
    
    logger.info("Success: " + str(successcount))
    logger.info("Failed: " + str(failurecount))
    logger.info("Total time: " + str(time.time() - start_time))


async def single_thread_ws():

    try:
        logger.info("Staring")
        
        print("Preparing Loading Payload")


        kamlload_start = time.time()

        with open("load_0000_1200.kaml", 'r') as file:
                kaml = file.read()

        kaml += "    - { \"command\": \"new context\" }"

        kamlload_end = str(time.time() - kamlload_start)
        print("Funning for {} rounds".format(f"{RUN_ROUNDS:,}"))
        print("Running press Ctr+C to abort.")    
        logger.info("Starting at " + str(datetime.now()))
        print("Starting at \t" + str(datetime.now()))

        transactions_rate = 0
        transactions_start = 0
        last_call = 0
        interval_in_sec = 1
        i = 0
        kaml_len = len(kaml)

        print(term.clear())


        async with websockets.connect(BASEURL_WS) as websocket:
            while True:
                post_start = time.time()
                await websocket.send(kaml)

                post_end = str(time.time() - post_start)

                logger.info(",{}".format(
                        post_end
                    )
                )
                    # print('processing', item[0])

                if (time.time() - last_call) > interval_in_sec:
                    transactions_rate = i - transactions_start
                    transactions_start = i
                    # transactions_rate = i - transactions_start
                    # transactions_start = i
                    last_call = time.time()

                with term.location():
                    try:
                        lock.acquire()
                        print(term.move(1, 0) + '[{}]: i={}, T/sec={}, data length={}'.format(1,i,transactions_rate, kaml_len))
                    finally:
                        lock.release()

                i += 1
    except KeyboardInterrupt:
        quitEvent.set()
        # try:
            # lock.acquire()
        with term.location(0, NUMBER_THREADS + 10):
            print('Interrupted!')
        # finally:
        #     lock.release()


def main():
    # single_thread()
    # single_thread_ws()
    asyncio.get_event_loop().run_until_complete(single_thread_ws())


if __name__ == "__main__":
    lock = Lock()
    logger = setup_custom_logger('loadtest')
    kaml = ""
    dataqueue = Queue()
    quitEvent = Event()


    # signal.signal(signal.SIGINT, signal_handler)
    if len(sys.argv) > 1:
        RESUME_INDEX=sys.argv[1]
        
    main()

