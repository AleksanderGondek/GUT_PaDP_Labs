"""
Parallel & Distributed Algorithms - laboratory

Examples:

- Launch 8 workers with default parameter values:
	> python arir.py 8

- Launch 12 workers with custom parameter values:
	> python arir.py 12 --shared-memory-size 128 --delay-connect 2.0 --delay-transmit 0.5 --delay-process 0.75

"""

__author__ = 'moorglade'

import multiprocessing
import time
import datetime
import sys
import argparse
import random
import math
from heapq import nlargest, nsmallest


def _parse_args():
    parser = argparse.ArgumentParser()

    # specify command line options
    parser.add_argument(
        'n_workers',
        help='number of workers in the distributed system',
        type=int
    )
    parser.add_argument(
        '--shared-memory-size',
        help='size of the shared memory array [number of ints]',
        type=int,
        default=16
    )
    parser.add_argument(
        '--delay-connect',
        help='network connection delay [s]',
        type=float,
        default=0.1
    )
    parser.add_argument(
        '--delay-transmit',
        help='network transmission delay [s]',
        type=float,
        default=0.1
    )
    parser.add_argument(
        '--delay-process',
        help='processing delay [s]',
        type=float,
        default=0.1
    )

    parser.add_argument(
        '--array-size',
        help='size of array to be sorted',
        type=int,
        default=24
    )

    return argparse.Namespace(**{
        key.replace('-', '_'): value
        for key, value in vars(parser.parse_args()).items()
    })


class DistributedSystem(object):
    def __init__(self, configuration):
        object.__init__(self)

        shared = SharedState(configuration.n_workers, configuration.shared_memory_size)
        network = Network(configuration)

        self.__workers = [
            Worker(worker_id, configuration, shared, network.get_endpoint(worker_id))
            for worker_id in range(configuration.n_workers)
        ]

    def run(self):
        print 'Launching {} workers...'.format(len(self.__workers))
        start = datetime.datetime.now()

        for worker in self.__workers:
            worker.start()

        print 'Waiting for the workers to terminate...'
        for worker in self.__workers:
            worker.join()

        stop = datetime.datetime.now()
        print 'All workers terminated.'

        print 'Processing took {} seconds.'.format((stop - start).total_seconds())


class SharedState(object):
    def __init__(self, n_workers, shared_memory_size):
        object.__init__(self)
        self.__barrier = Barrier(n_workers)
        self.__memory = multiprocessing.Array('i', shared_memory_size)

    @property
    def barrier(self):
        return self.__barrier

    @property
    def memory(self):
        return self.__memory


class Barrier(object):
    def __init__(self, n):
        object.__init__(self)
        self.__counter = multiprocessing.Value('i', 0, lock=False)
        self.__n = n
        self.__condition = multiprocessing.Condition()

    def wait(self):
        with self.__condition:
            self.__counter.value += 1

            if self.__counter.value == self.__n:
                self.__counter.value = 0
                self.__condition.notify_all()

            else:
                self.__condition.wait()


class SharedMemory(object):
    def __init__(self, shared_memory_size):
        object.__init__(self)
        self.__array = multiprocessing.Array('i', shared_memory_size)


class Network(object):
    any_id = -1

    def __init__(self, configuration):
        object.__init__(self)
        channels = [NetworkChannel(configuration) for _ in range(configuration.n_workers)]
        self.__endpoints = [NetworkEndpoint(channel_id, channels) for channel_id in range(configuration.n_workers)]

    def get_endpoint(self, index):
        return self.__endpoints[index]


class NetworkChannel(object):
    def __init__(self, configuration):
        self.__configuration = configuration

        self.__source_id = multiprocessing.Value('i', Network.any_id, lock=False)
        self.__queue = multiprocessing.Queue(maxsize=1)
        self.__enter_lock = multiprocessing.Lock()
        self.__exit_lock = multiprocessing.Lock()
        self.__enter_lock.acquire()
        self.__exit_lock.acquire()

    def send(self, source_id, data):
        while True:
            self.__enter_lock.acquire()

            if self.__source_id.value in [source_id, Network.any_id]:
                self.__source_id.value = source_id
                self.__queue.put(data)
                time.sleep(self.__configuration.delay_connect + len(data) * self.__configuration.delay_transmit)
                self.__exit_lock.release()
                break

            else:
                self.__enter_lock.release()

    def receive(self, source_id=Network.any_id):
        self.__source_id.value = source_id

        self.__enter_lock.release()
        data = self.__queue.get()
        self.__exit_lock.acquire()

        return self.__source_id.value, data


class NetworkEndpoint(object):
    def __init__(self, channel_id, channels):
        self.__channels = channels
        self.__my_id = channel_id
        self.__my_channel = self.__channels[self.__my_id]

    def send(self, destination_id, data):
        if destination_id == self.__my_id:
            raise RuntimeError('Worker {} tried to send data to itself.'.format(self.__my_id))

        self.__channels[destination_id].send(self.__my_id, data)

    def receive(self, worker_id=Network.any_id):
        return self.__my_channel.receive(worker_id)


class Worker(multiprocessing.Process):
    def __init__(self, worker_id, configuration, shared, network_endpoint):
        multiprocessing.Process.__init__(self)

        self.__worker_id = worker_id
        self.__configuration = configuration
        self.__shared = shared
        self.__network_endpoint = network_endpoint

        self.data = None
        self.data_chunk_size = int(math.floor(self.__configuration.array_size / self.__configuration.n_workers))
        self.dimensions = int(math.floor(math.log(self.__configuration.n_workers, 2)))

    @property
    def __n_workers(self):
        return self.__configuration.n_workers

    def __barrier(self):
        self.__shared.barrier.wait()

    def _send(self, worker_id, data):
        self.__network_endpoint.send(worker_id, data)

    def _receive(self, worker_id=Network.any_id):
        return self.__network_endpoint.receive(worker_id)

    @staticmethod
    def __generate_random_data(length):
        return [random.randint(-2048, 2048) for _ in range(length)]

    def __log(self, message):
        print '[WORKER {}] {}'.format(self.__worker_id, message)

    def __process(self, data):
        # simulates data processing delay by sleeping
        time.sleep(len(data) * self.__configuration.delay_process)

    # MY METHODS
    def quick_sort(self, items):
        """
        Implementation of quick sort
        """
        if len(items) > 1:
            pivot_index = len(items) / 2
            smaller_items = []
            larger_items = []

            for i, val in enumerate(items):
                if i != pivot_index:
                    if val < items[pivot_index]:
                        smaller_items.append(val)
                    else:
                        larger_items.append(val)

            self.quick_sort(smaller_items)
            self.quick_sort(larger_items)
            items[:] = smaller_items + [items[pivot_index]] + larger_items

        return items

    def merge(self, a, b, from_top):
        merged = []
        merged.extend(a)
        merged.extend(b)

        merged = self.quick_sort(merged)
        if from_top:
            return merged[len(a):len(merged)]
        else:
            return merged[0:len(a)]

        # not sorting the whole bloody array :)
        #return nlargest(len(a), merged) if from_top else nsmallest(len(a), merged)

    def handle_data_creation_and_distribution(self):
        # Master process
        if self.__worker_id == 0:
            data_to_be_sorted = self.__generate_random_data(self.__configuration.array_size)

            self.__log('Transmitting parts of data to other workers. Size of each chunk: {}'.format(self.data_chunk_size))

            # Self assign first chunk of data
            start_chunk_index = 0
            self.data = data_to_be_sorted[start_chunk_index:(start_chunk_index + self.data_chunk_size)]
            start_chunk_index += self.data_chunk_size

            # Broadcast data chunks
            for worker_id in range(1, self.__n_workers):
                self._send(worker_id, data_to_be_sorted[start_chunk_index:(start_chunk_index + self.data_chunk_size)])
                start_chunk_index += self.data_chunk_size

        # Slave
        else:
            source_id, data = self._receive()
            self.data = data
            self.__log('Received data from worker {}: {}'.format(source_id, data))

        # Wait for all
        self.__barrier()

    def handle_data_gather_and_print(self):
        sorted_data_complete = []

        # Master process
        if self.__worker_id == 0:
            self.__log('Receiving data from workers...')

            for worker_id in range(1, self.__n_workers):
                source_id, data = self._receive(worker_id)
                self.__log('Received data from worker {}: {}'.format(source_id, data))
                sorted_data_complete.extend(data)

        else:
            self._send(0, self.data)

        self.__barrier()

    def get_partner(self, rank, j):
        # Partner process is process with j_th bit of rank flipped
        j_mask = 1 << j
        partner = rank ^ j_mask
        return partner

    def compare_high(self, j):
        partner = self.get_partner(self.__worker_id, j)

        self._send(partner, self.data)
        source, new_data = self._receive()

        self.data = self.merge(self.data, new_data, True)

    def compare_low(self, j):
        partner = self.get_partner(self.__worker_id, j)

        source, new_data = self._receive()
        self._send(partner, self.data)

        self.data = self.merge(self.data, new_data, False)

    def run(self):
        self.__log('Started.')

        self.handle_data_creation_and_distribution()
        # Sequential sort
        # No diff without this, i don't even
        self.data = self.quick_sort(self.data)

        # Generating map of bit_j for each process.
        bit_j = [0 for i in range(self.dimensions)]
        for i in range(self.dimensions):
            bit_j[i] = (self.__worker_id >> i) & 1

        for i in range(1, self.dimensions + 1):
            window_id = self.__worker_id >> i
            for j in reversed(range(0, i)):
                if self.__worker_id == 0:
                    self.__log("[{}] iteration {}, {}".format(self.__worker_id, i, j))

                self.__barrier()

                if (window_id % 2 == 0 and bit_j[j] == 0) or (window_id % 2 == 1 and bit_j[j] == 1):
                    self.compare_low(j)
                else:
                    self.compare_high(j)

                self.__barrier()

        self.handle_data_gather_and_print()

        self.__log('Terminated.')


def main():
    random.seed()
    configuration = _parse_args()
    system = DistributedSystem(configuration)
    system.run()


if __name__ == '__main__':
    sys.exit(main())
