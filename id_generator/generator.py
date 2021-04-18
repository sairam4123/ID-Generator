import random
import time

ID_EPOCH = 1577836800


pid = 1
wid = 1

#
# current_time = int(time.time())
# print(current_time)
# starting_id = (current_time - ID_EPOCH) << 22
# print(starting_id, len(str(starting_id)))
# starting_id = (starting_id << 17) | os.getpid()
# print(starting_id, len(str(starting_id)))
# starting_id = (starting_id << 12) | 65535
# print(starting_id, len(str(starting_id)))
# print(bin(starting_id), len(str(bin(starting_id))))
# final_id = (((int(datetime.datetime.utcnow().timestamp() * 1000) << 22) - 16094394000000) | os.getpid() | 65535)
# print(final_id)
# print(((final_id // 1000) >> 22) + ID_EPOCH)
# print((starting_id >> 22) + ID_EPOCH)
# datetime.datetime.utcfromtimestamp(0)

# 00000000000000000000000000000000000000000|00000000000000000000|0000000000000
#              Timestamp                    ProcessID    Increment

# print(pid)
# current_time = int(time.time())
# increment = 0
# _process_id_bits = 5
# _worker_id_bits = 5
# _increment_bits = 12
# timestamp_left_shift = _process_id_bits + _worker_id_bits + _increment_bits
# process_id_left_shift = _worker_id_bits + _increment_bits
# worker_id_left_shift = _increment_bits
# #
# starting_id = ((current_time - ID_EPOCH) << timestamp_left_shift) | (pid << process_id_left_shift) | (wid << worker_id_left_shift) | increment
# print((current_time - ID_EPOCH))
# print(datetime.datetime.utcfromtimestamp(((starting_id >> 22) + ID_EPOCH)) + datetime.timedelta(0, hours=5, minutes=30))
# # print(increment, _pr, worker_id_bits, max_worker_id,  max_process_id, max_increment_id)
# print((starting_id & 0x3E0000) >> 17)
# print((starting_id & 0x1F000) >> 12)
# print((starting_id & 0xFFF))
#

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
    def __init__(self, epoch, process_id_bits=5, worker_id_bits=5, increment_bits=5):
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

        self.workers = []
        self.workers_full = []

    def create_process_and_worker(self, worker_id, process_id, increment=0) -> 'IDProcessWorker':
        worker = IDProcessWorker(worker_id, process_id, self.epoch, self, increment)
        self.workers.append(worker)
        self.workers_full.append(worker)
        return worker

    def create_id(self) -> int:
        random.shuffle(self.workers)
        random_worker = self.workers.pop(0)
        if not self.workers:
            self.workers = self.workers_full.copy()
        return random_worker.get_next_id()

    def get_worker(self, worker_id, process_id) -> 'IDProcessWorker':
        for worker in self.workers_full:
            if worker.worker_id == worker_id and worker.process_id == process_id:
                return worker


if __name__ == '__main__':
    id_gen_1 = IDGenerator(ID_EPOCH)

    for j in range(4):
        for i in range(10):
            id_gen_1.create_process_and_worker(i, j)

    id_array = []
    for j in range(100000):
        created_id = id_gen_1.create_id()
        # print(created_id)
        id_array.append(created_id)

    print(len(set(id_array)), len(id_array))
    #
    # starting_id = id_gen_1.create_id()
    # print(starting_id)
    # print(datetime.datetime.utcfromtimestamp(int(((starting_id >> 22) + ID_EPOCH * 1000.0) / 1000.0)) + datetime.timedelta(hours=5, minutes=30))
    # print((starting_id & 0x3E0000) >> 17)
    # print((starting_id & 0x1F000) >> 12)
    # print((starting_id & 0xFFF))
    #
    # print(bin(starting_id))
    # # # print( + datetime.timedelta(0, hours=5, minutes=30))
    # # starting_id = id_gen_1.create_id()
    # # print(starting_id)
    # # print(datetime.datetime.utcfromtimestamp(((starting_id >> 22) + ID_EPOCH)) + datetime.timedelta(0, hours=5, minutes=30))
    # # starting_id = id_gen_1.create_id()
    # # print(starting_id)
    # # print(datetime.datetime.utcfromtimestamp(((starting_id >> 22) + ID_EPOCH)) + datetime.timedelta(0, hours=5, minutes=30))
    #
    # print((int(time.time() * 1000.0) - ID_EPOCH * 1000.0)/1000.0)  # 1000.0)
    # # int(time.time() * 1000.0
    # print(0b100110000111110101010100011110101111)