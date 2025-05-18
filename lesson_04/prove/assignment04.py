"""
Course    : CSE 351
Assignment: 04
Student   : Austin Virgin

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}'

where:

name: name of the city
recno: record number starting from 0

"""

import time
from common import *

from cse351 import *

import threading

import queue

THREADS = 100                 # TODO - set for your program
WORKERS = 100
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(id, m_q, t_q, m_q_semaphore, t_q_semaphore, thread_barrier, t_q_r, m_q_r):

    m_q_r.acquire()
    city = m_q.get()
    q_info(city, t_q)
    m_q_semaphore.release()
    t_q_r.release()

    thread_barrier.wait()

    while True:
        m_q_r.acquire()
        t_q_semaphore.acquire()
        city = m_q.get()
        if city is None:
            break

        q_info(city, t_q)
        m_q_semaphore.release()
        t_q_r.release()

    thread_barrier.wait()
    if id == 0:
        for _ in range(WORKERS):
            t_q.put(None)
            t_q_r.release()
    print(t_q.qsize())

def q_info(city,t_q):
    name = city[0]
    recno = city[1]
    q = get_data_from_server(f'{TOP_API_URL}/record/{name}/{recno}')
    t_q.put(q)


# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class Worker_class(threading.Thread):
    def __init__(self, t_q_r, t_q_s, t_q, noaa):
        super().__init__()
        self.tqr = t_q_r
        self.tqs = t_q_s
        self.tq = t_q
        self.noaa = noaa

    def run(self):
        while True:
            self.tqr.acquire()
            city = self.tq.get()
            if city is None:
                break
            self.store_data(city)

    def store_data(self, city):
       self.noaa.add_city(city)
       self.tqs.release()



# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:

    def __init__(self):
        self.cities = {}
        self.new_dict = threading.Lock()
        self.add = threading.Lock()

    def add_city(self, city):
        city_name = city['city']
        city_temp = city['temp']

        if city_name not in self.cities:
            with self.new_dict:
                if city_name not in self.cities:
                    self.cities[city_name] = {"amount": 0, "temp": 0}
        with self.add:
            self.cities[city_name]['amount'] += 1
            self.cities[city_name]['temp'] += city_temp

    def get_temp_details(self, city):
        return self.cities[city]['temp']/self.cities[city]['amount']


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need
    main_queue = queue.Queue()
    main_queue_ready = threading.Semaphore(0)
    thread_queue_ready = threading.Semaphore(0)
    thread_queue = queue.Queue()
    main_queue_semaphore = threading.Semaphore(THREADS)
    thread_queue_semaphore = threading.Semaphore(WORKERS)
    thread_barrier = threading.Barrier(THREADS)

    retrievers = []
    for i in range(THREADS):
        r = threading.Thread(target=retrieve_weather_data, args=(i, main_queue, thread_queue, main_queue_semaphore, thread_queue_semaphore, thread_barrier, thread_queue_ready, main_queue_ready))
        r.start()
        retrievers.append(r)

    workers = []
    for _ in range(WORKERS):
        w = Worker_class(thread_queue_ready, thread_queue_semaphore, thread_queue, noaa)
        w.start()
        workers.append(w)

    for record_num in range(records):
        for city in city_details:
            save_record_to_queue(city, record_num, main_queue, main_queue_semaphore, main_queue_ready)

    for i  in range(THREADS):
        main_queue_semaphore.acquire()
        main_queue.put(None)
        main_queue_ready.release()

    for thread in retrievers + workers:
        thread.join()

    print(main_queue.qsize())

    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


def save_record_to_queue(place, num, main_queue, semaphore, main_queue_ready):
    semaphore.acquire()
    main_queue.put((place, num))
    main_queue_ready.release()

if __name__ == '__main__':
    main()

