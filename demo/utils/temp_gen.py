import random
from datetime import datetime


def temp_measures():
    """Returns a generator that simulate temperature measurements along time"""
    temp = random.uniform(20.0, 25.0)
    while True:
        temp_diff = random.random()
        temp_increment = random.choice([True, False])
        if temp_increment:
            temp += temp_diff
        else:
            temp -= temp_diff

        ts = datetime.now().strftime("%H:%M:%S")
        yield ts, round(temp, 1)
