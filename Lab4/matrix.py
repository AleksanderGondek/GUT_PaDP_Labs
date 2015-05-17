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
        '--submatrix-size',
        help='single square sub-matrix size',
        type=int,
        default=3
    )
    parser.add_argument(
        '--should-count-square',
        help='should compute square root of matrix A or AxB multiplication',
        type=bool,
        default=True
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


class Matrix(object):
    def __init__(self, size, is_counting_square, id):
        # self.dataA = [[random.randint(-2048, 2048) for x in range(size)] for x in range(size)]
        self.dataA = [[id for x in range(size)] for x in range(size)]
        if is_counting_square:
            self.dataB = self.dataA
        else:
            self.dataB = [[random.randint(-2048, 2048) for x in range(size)] for x in range(size)]
        self.dataC = [[0 for x in range(size)] for x in range(size)]

    @staticmethod
    def get_data_of_shift(matrix, direction):
        if direction == 'left':
            return matrix[0]
        elif direction == 'right':
            return matrix[-1]
        elif direction == 'up':
            return list(zip(*matrix)[0])
        elif direction == 'down':
            return list(zip(*matrix)[-1])

        return None

    @staticmethod
    def multiply(matrix_a, matrix_b):
        return [[sum(a*b for a, b in zip(X_row, Y_col)) for Y_col in zip(*matrix_b)] for X_row in matrix_a]


class Worker(multiprocessing.Process):
    def __init__(self, worker_id, configuration, shared, network_endpoint):
        multiprocessing.Process.__init__(self)

        self.__worker_id = worker_id
        self.__configuration = configuration
        self.__shared = shared
        self.__network_endpoint = network_endpoint

        self.carts = Worker.get_carts_layout(configuration.n_workers)
        self.matrix = Matrix(configuration.submatrix_size, configuration.should_count_square, worker_id)

    @property
    def _n_workers(self):
        return self.__configuration.n_workers

    @property
    def _sub_matrix_size(self):
        return self.__configuration.submatrix_size

    @staticmethod
    def _generate_random_data(length):
        return [random.randint(-2048, 2048) for _ in range(length)]

    def _barrier(self):
        self.__shared.barrier.wait()

    def _send(self, worker_id, data):
        self.__network_endpoint.send(worker_id, data)

    def _receive(self, worker_id=Network.any_id):
        return self.__network_endpoint.receive(worker_id)

    def _log(self, message):
        print '[WORKER {}] {}'.format(self.__worker_id, message)

    # My methods

    # Getting current cart id
    def get_cart_row_id(self):
        return self.get_my_cart_coordinates()[0]

    def get_cart_column_id(self):
        return self.get_my_cart_coordinates()[1]

    # Returns list of matches, so assuming there is only one, we need to take first element
    def get_my_cart_coordinates(self):
        return [(ind, self.carts[ind].index(self.__worker_id)) for ind in xrange(len(self.carts)) if self.__worker_id in self.carts[ind]][0]

    # Shifting of meshes between carts
    # Constitutive name - we shift columns, left
    def left_circular_shift_row(self):
        my_column_id = self.get_cart_column_id()
        last_column_id = int(math.sqrt(self._n_workers)) - 1

        if my_column_id == 0:
            # Last column
            my_send_partner = self.carts[self.get_cart_row_id()][last_column_id]
            my_receive_partner = self.carts[self.get_cart_row_id()][my_column_id + 1]
        elif my_column_id == last_column_id:
            # first column
            my_send_partner = self.carts[self.get_cart_row_id()][my_column_id - 1]
            my_receive_partner = self.carts[self.get_cart_row_id()][0]
        else:
            my_send_partner = self.carts[self.get_cart_row_id()][my_column_id - 1]
            my_receive_partner = self.carts[self.get_cart_row_id()][my_column_id + 1]

        received_data = None
        if (my_column_id % 2) == 0:
            source, received_data = self._receive(my_receive_partner)
        else:
            self._send(my_send_partner, self.matrix.dataA)
        if (my_column_id % 2) == 1:
            source, received_data = self._receive(my_receive_partner)
        else:
            self._send(my_send_partner, self.matrix.dataA)

        self.matrix.dataA = received_data

    # Shifting of meshes between carts
    # Constitutive name - we shift rows, up
    def upward_circular_shift_column(self):
        my_row_id = self.get_cart_row_id()
        last_row_id = int(math.sqrt(self._n_workers)) - 1

        if my_row_id == 0:
            # Last column
            my_send_partner = self.carts[last_row_id][self.get_cart_column_id()]
            my_receive_partner = self.carts[my_row_id + 1][self.get_cart_column_id()]
        elif my_row_id == last_row_id:
            # first column
            my_send_partner = self.carts[my_row_id - 1][self.get_cart_column_id()]
            my_receive_partner = self.carts[0][self.get_cart_column_id()]
        else:
            my_send_partner = self.carts[my_row_id - 1][self.get_cart_column_id()]
            my_receive_partner = self.carts[my_row_id + 1][self.get_cart_column_id()]

        received_data = None
        if (my_row_id % 2) == 0:
            source, received_data = self._receive(my_receive_partner)
        else:
            self._send(my_send_partner, self.matrix.dataB)
        if (my_row_id % 2) == 1:
            source, received_data = self._receive(my_receive_partner)
        else:
            self._send(my_send_partner, self.matrix.dataB)

        self.matrix.dataB = received_data

    @staticmethod
    def get_carts_layout(number_of_carts):
        carts = [[0 for y in range(int(math.sqrt(number_of_carts)))] for x in range(int(math.sqrt(number_of_carts)))]

        counter = 0
        for x in range(len(carts)):
            for y in range(len(carts)):
                carts[x][y] = counter
                counter += 1

        return carts

    def run(self):
        #self._log('Started.')
        # self._log(repr(self.matrix.dataA))
        self.left_circular_shift_row()
        self._log(repr(self.matrix.dataA))
        # self._log('Terminated.')


def main():
    random.seed()
    configuration = _parse_args()
    system = DistributedSystem(configuration)
    system.run()


if __name__ == '__main__':
    sys.exit(main())
