from json import dumps
from time import sleep
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
import random
from os import getpid
from datetime import datetime
import cv2 as cv
import numpy as np
import matplotlib.pyplot as plt
from datetime import date, datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

width = random.choice([16, 32])


def gen_image():
    imarray = (np.random.rand(width, width, 3) * 255).astype(np.uint8)
    return imarray

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

for e in range(2000):
    data = {'id' : getpid(), 'date': dumps(datetime.now(), default=json_serial), 'data': dumps(gen_image().tolist())}
    producer.send('my-topic', value=data)
    sleep(5)
