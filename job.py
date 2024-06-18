import numpy as np
import configparser

class Job:
    def __init__(self):
        self.id = None
        self.processing_time = None
        self.arrival_time = None
        self.resources = None
        self.start_time = None
        self.end_time = None
        self.done = False
        self.send_time = None
        self.config = configparser.ConfigParser()

    def generate_self(self, id, send_time):
        self.config.read('config.properties')
        max_res = self.config.get('SCHEDULER', 'MAX_RES')

        self.resources = np.random.randint(1, max_res)
        self.id = id
        self.processing_time = int(np.random.poisson(5, 1)[0])
        self.send_time = send_time


    def to_dict(self):
        return {
            'id': self.id,
            'processing_time': self.processing_time,
            'arrival_time': self.arrival_time,
            'resources': self.resources,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'done': self.done,
            'send_time': self.send_time
        }

