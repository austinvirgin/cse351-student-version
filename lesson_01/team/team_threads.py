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
import math

# Include cse 351 common Python files
from cse351 import *

# Global variable for counting the number of primes found
prime_count = 0
numbers_processed = 0 

def for_loop(start, range_count):
    global prime_count
    global numbers_processed 
    for i in range(start, start + range_count):
        numbers_processed += 1
        if is_prime(i):
            prime_count += 1
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

    start = 10000000000
    range_count = random.randint(100000, 1000000)
    numbers_processed = 0

    range_count_in_10 = range_count/10

    range_count_in_10 = math.floor(range_count_in_10)

    range_count_in_10_last = range_count_in_10 + range_count % 10

    one = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    two = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    three = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    four = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    five = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    six = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    seven = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    eight = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    nine = threading.Thread(target=for_loop, args=(start, range_count_in_10))
    start += range_count_in_10
    ten = threading.Thread(target=for_loop, args=(start, range_count_in_10_last))
    one.start()
    two.start()
    three.start()
    four.start()
    five.start()
    six.start()
    seven.start()
    eight.start()
    nine.start()
    ten.start()
    one.join()
    two.join()
    three.join()
    four.join()
    five.join()
    six.join()
    seven.join()
    eight.join()
    nine.join()
    ten.join()
    print(flush=True)

    # Should find 4306 primes
    log.write(f'Numbers processed = {numbers_processed}')
    log.write(f'Primes found      = {prime_count}')
    log.stop_timer('Total time')


if __name__ == '__main__':
    main()
