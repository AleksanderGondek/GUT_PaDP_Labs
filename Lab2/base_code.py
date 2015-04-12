"""
Parallel & Distributed Algorithms - laboratory

Examples:

- Launch 8 workers with default parameter values:
	> python arir.py 8

- Launch 12 workers with custom parameter values:
	> python arir.py 12 --shared-memory-size 128 --delay-connect 2.0 --delay-transmit 0.5 --delay-process 0.75

"""

# Total sum, Prefix
# prefix increased only if sender index is lower than mine
# neighbouirs are nodes which binary index differ by one (i.e. 0110 neighbours - 0111, 0100, 0010, 1110

__author__ = 'moorglade'

import multiprocessing
import time
import datetime
import sys
import argparse
import random
import math


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
        self.__my_iteration = 0

        my_number = random.randint(0, 2048)
        self.__total = my_number
        self.__prefix = my_number

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

    # My methods
    def _get_number_of_iterations_needed(self):
        iterations = math.log(self.__n_workers, 2.0)
        iterations = math.floor(iterations)
        return int(iterations)

    def _get_target_node_index(self):
        index = int(math.pow(2.0, self.__my_iteration)) ^ self.__worker_id
        return int(index)

    def run(self):
        self.__log('Started.')

        iterations_needed = self._get_number_of_iterations_needed()
        self.__log('{} iterations needed'.format(iterations_needed))
        while self.__my_iteration < iterations_needed:
            iteration_even = self.__my_iteration % 2
            self.__log('Iteration {} started.'.format(self.__my_iteration))
            source_id = None
            data = None

            if self.__worker_id % 2 == iteration_even:
                target_id = self._get_target_node_index()
                self.__log('wololo {}'.format(target_id))
                self._send(target_id, [self.__total])
                self.__log('Proc {0} send number {1} to proc {2}'.format(self.__worker_id, self.__total, target_id))
            else:
                source_id, data = self._receive()

            self.__barrier()

            if self.__worker_id % 2 == int(not bool(iteration_even)):
                target_id = self._get_target_node_index()
                self.__log('wololo {}'.format(target_id))
                self._send(target_id, [self.__total])
                self.__log('Proc {0} send number {1} to proc {2}'.format(self.__worker_id, self.__total, target_id))
            else:
                source_id, data = self._receive()

            self.__total += data[0]
            if source_id < self.__worker_id:
                self.__prefix += data[0]

            self.__my_iteration += 1
            self.__barrier()


        # TODO: this is the main method to implement

        # ============================================================================================================ #
        # Example 1 - simple broadcast
        #
        # Description:
        # Worker 0 sends random data to all other workers.
        # ============================================================================================================ #
        #
        # if self.__worker_id == 0:
        # data = Worker.__generate_random_data(16)
        #
        # self.__log('Transmitting data to other workers: {}'.format(data))
        #
        # for worker_id in range(1, self.__n_workers):
        # 		self._send(worker_id, data)
        #
        # else:
        # 	source_id, data = self._receive()
        # 	self.__process(data)
        # 	self.__log('Received data from worker {}: {}'.format(source_id, data))
        #
        # ============================================================================================================ #

        # ============================================================================================================ #
        # Example 2 - ordered gather
        #
        # Description:
        # Worker 0 receives data from consecutive workers (1, 2, 3, ..., N-1)
        # ============================================================================================================ #
        #
        # if self.__worker_id == 0:
        # 	self.__log('Receiving data from workers...')
        #
        # 	for worker_id in range(1, self.__n_workers):
        # 		source_id, data = self._receive(worker_id)
        # 		self.__process(data)
        # 		self.__log('Received data from worker {}: {}'.format(source_id, data))
        #
        # else:
        # 	data = [self.__worker_id]
        # 	time.sleep(random.uniform(0.0, 1.0))
        # 	self._send(0, data)
        #
        # ============================================================================================================ #

        # ============================================================================================================ #
        # Example 3 - barrier synchronization
        #
        # Description:
        # All workers wait on a shared barrier.
        # ============================================================================================================ #
        #
        # time.sleep(random.uniform(0.0, 5.0))
        # self.__barrier()
        #
        # ============================================================================================================ #

        # ============================================================================================================ #
        # Example 4 - shared memory access
        #
        # Description:
        # All workers add their private data to the data array stored in the shared memory.
        # ============================================================================================================ #
        #
        # data_length = len(self.__shared.memory)
        # private_data = [i * self.__worker_id for i in range(data_length)]
        # self.__log('Private data: {}'.format(private_data))
        #
        # with self.__shared.memory.get_lock():
        # 	for i in range(data_length):
        # 		self.__shared.memory[i] += private_data[i]
        #
        # self.__barrier()
        #
        # with self.__shared.memory.get_lock():
        # 	if self.__worker_id == 0:
        # 		self.__log('Shared data: {}'.format(self.__shared.memory[:]))
        #
        # ============================================================================================================ #

        self.__log('Terminated.')


def main():
    random.seed()
    configuration = _parse_args()
    system = DistributedSystem(configuration)
    system.run()


if __name__ == '__main__':
    sys.exit(main())
