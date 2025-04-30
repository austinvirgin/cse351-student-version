""" 
Course: CSE 351
Lesson: L01 Team Activity
File:   team.py
Author: <Add name here>
Purpose: Find prime numbers

Instructions:

- Don't include any other Python packages or modules
- Review and follow the team activity instructions (team.md)

TODO 1) Get this program running.  Get cse351 package installed
TODO 2) move the following for loop into 1 thread
TODO 3) change the program to divide the for loop into 10 threads
TODO 4) change range_count to 100007.  Does your program still work?  Can you fix it?
Question: if the number of threads and range_count was random, would your program work?
"""

from datetime import datetime, timedelta
import threading
import random

# Include cse 351 common Python files
from cse351 import *

# Global variable for counting the number of primes found
prime_count = 0
numbers_processed = 0 

class primeThread(threading.Thread):
    def __init__(self, starter, range_count, num_lock):
        super().__init__()
        self.num_primes = 0
        self.range_count = range_count
        self.num_lock = num_lock
        self.starter = starter

    def run(self): 
        for i in range(self.starter, self.starter + self.range_count):
            self.num_lock.acquire()
            try:
                numbers_processed += 1
            finally:
                self.num_lock.release()
            if is_prime(i):
                with self.prime_lock:
                    num_primes += 1
                print(i, end=', ', flush=True)



def is_prime(n):
    """
        Primality test using 6k+-1 optimization.
        From: https://en.wikipedia.org/wiki/Primality_test
    """
    if n <= 3:
        return n > 1
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i ** 2 <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def main():
    global prime_count                  # Required in order to use a global variable
    global numbers_processed            # Required in order to use a global variable

    log = Log(show_terminal=True)
    log.start_timer()

    start = 10_000_000_000
    range_count = 100000
    numbers_processed = 0
    num_threads = 8

    prime_lock = threading.Lock()

    numbers_lock = threading.Lock()

    threads = []

    range_count_in_10_mod =  range_count % num_threads

    range_count_in_10 = int((range_count - range_count_in_10_mod) / num_threads)

    range_count_in_10_last = range_count_in_10 + range_count_in_10_mod

    for i in range(1, num_threads + 1):
        if i == num_threads - 1:
            t = threading.Thread(target=for_loop, args=(start, range_count_in_10_last, prime_lock, numbers_lock))
        else:
            t = threading.Thread(target=for_loop, args=(start, range_count_in_10, prime_lock, numbers_lock))
        start += range_count_in_10
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print(flush=True)
    print(range_count)
    print(num_threads)

    # Should find 4306 primes
    log.write(f'Numbers processed = {numbers_processed}')
    log.write(f'Primes found      = {prime_count}')
    log.stop_timer('Total time')


if __name__ == '__main__':
    main()
