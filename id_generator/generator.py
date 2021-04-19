import random
import time
from typing import List


class IDProcessWorker:
    def __init__(self, worker_id, process_id, epoch, id_gen: 'IDGenerator', increment=0):
        self.worker_id = worker_id
        self.process_id = process_id
        self.epoch = int(epoch * 1000.0)
        self.id_gen = id_gen
        self.last_timestamp = -1
        self.increment = increment
        self.timestamp = self.current_time()

    def get_next_id(self) -> int:
        timestamp = self.current_time()
        if timestamp < self.last_timestamp:
            raise OSError("Time running backwards, not generating ids till %d milliseconds." % (self.last_timestamp - timestamp))

        self.increment = (self.increment + 1) & self.id_gen.max_increment_id
        self.last_timestamp = timestamp

        return int(((timestamp - self.epoch) << self.id_gen.timestamp_left_shift)
                   | (self.process_id << self.id_gen.process_id_left_shift)
                   | (self.worker_id << self.id_gen.worker_id_left_shift)
                   | self.increment)

    def next_milliseconds(self, last_timestamp) -> int:
        timestamp = self.current_time()
        while timestamp <= last_timestamp:
            timestamp = self.current_time()
        return timestamp

    @staticmethod
    def current_time():
        return int(time.time() * 1000.0)


class IDGenerator:
    def __init__(self, epoch: int, process_id_bits=5, worker_id_bits=5, increment_bits=12, random_worker=True):
        # ID bits
        self.process_id_bits = process_id_bits
        self.worker_id_bits = worker_id_bits
        self.increment_bits = increment_bits
        # Max ids
        self.max_process_id = -1 ^ (-1 << self.process_id_bits)
        self.max_increment_id = -1 ^ (-1 << self.increment_bits)
        self.max_worker_id = -1 ^ (-1 << self.worker_id_bits)

        # Left shifts
        self.worker_id_left_shift = self.increment_bits
        self.process_id_left_shift = self.worker_id_bits + self.worker_id_left_shift
        self.timestamp_left_shift = self.process_id_bits + self.process_id_left_shift
        self.epoch = epoch

        self.workers: List['IDProcessWorker'] = []
        self.workers_full: List['IDProcessWorker'] = []
        self.random_worker = random_worker
        if not self.random_worker:
            self.serial_worker_index = 0

    def create_process_and_worker(self, worker_id, process_id, increment=0) -> 'IDProcessWorker':
        worker = IDProcessWorker(worker_id, process_id, self.epoch, self, increment)
        self.workers.append(worker)
        self.workers_full.append(worker)
        return worker

    def create_id(self) -> int:
        if self.random_worker:
            random.shuffle(self.workers)
            random_worker = self.workers.pop(0)
            if not self.workers:
                self.workers = self.workers_full.copy()
            return random_worker.get_next_id()
        else:
            serial_worker = self.workers_full[self.serial_worker_index]
            self.serial_worker_index %= len(self.workers_full)
            serial_worker.get_next_id()

    def get_worker(self, worker_id, process_id) -> 'IDProcessWorker':
        for worker in self.workers_full:
            if worker.worker_id == worker_id and worker.process_id == process_id:
                return worker


if __name__ == '__main__':
    import datetime

    EPOCH = 1577836800  # 1st Jan 2020

    id_gen_1 = IDGenerator(EPOCH)  # Make sure not to pass EPOCH as milliseconds.

    # Create some workers. This snippet creates 40 workers, creating 4 processes containing 10 workers in each process.
    for j in range(4):
        for i in range(10):
            id_gen_1.create_process_and_worker(i, j)  # You must create a worker to create IDs.

    # Create ID
    starting_id = id_gen_1.create_id()
    print("Recent ID:", starting_id)
    # Get UTC Time
    utc_time_stamp = int(((starting_id >> 22) + (EPOCH * 1000.0)) / 1000.0)  # Divide by 1000.0 to get seconds.
    print("UTC Timestamp:", utc_time_stamp)
    #                                                    ^ Multiply by 1000.0 to get milliseconds, as the timestamp is in millisecond precision.
    utc_time = datetime.datetime.utcfromtimestamp(utc_time_stamp).replace(tzinfo=datetime.timezone.utc)
    print("UTC Time:", utc_time)
    print("Local Time:", utc_time.astimezone(tz=None))
    print("Process ID:", (starting_id & 0x3E0000) >> 17)
    print("Worker ID:", (starting_id & 0x1F000) >> 12)
    print("Increment:", (starting_id & 0xFFF))
