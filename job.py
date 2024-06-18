import numpy as np
import configparser

# job.py

import numpy as np

class Job:
    def __init__(self):
        self.id = None
        self.processing_time = None
        self.arrival_time = None
        self.resources = None
        self.start_time = None
        self.end_time = None
        self.done = False
        self.config = None
        self.max_res = None
        self.send_time = None

    def generate_self(self, id, send_time):
        # Simulating generation of processing time and resources
        self.id = id
        self.processing_time = int(np.random.poisson(5, 1)[0])  # Convert to int
        self.send_time = send_time
        self.resources = int(np.random.randint(1, self.max_res))  # Convert to int

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

