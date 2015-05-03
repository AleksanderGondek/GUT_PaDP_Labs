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

        self.__my_data = None
        self.__data_per_process_size = int(math.floor(self.__configuration.array_size / self.__configuration.n_workers))
        self.__dimensions = int(math.floor(math.log(self.__configuration.n_workers, 2)))

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

    def handle_data_creation_and_distribution(self):
        # Master process
        if self.__worker_id == 0:
            data_to_be_sorted = self.__generate_random_data(self.__configuration.array_size)

            self.__log('Transmitting parts of data to other workers. Size of each chunk: {}'.format(
                self.__data_per_process_size))

            # Self assign first chunk of data
            start_chunk_index = 0
            self.__my_data = data_to_be_sorted[start_chunk_index:(start_chunk_index + self.__data_per_process_size)]
            start_chunk_index += self.__data_per_process_size

            # Broadcast data chunks
            for worker_id in range(1, self.__n_workers):
                self._send(worker_id,
                           data_to_be_sorted[start_chunk_index:(start_chunk_index + self.__data_per_process_size)])
                start_chunk_index += self.__data_per_process_size

        # Slave
        else:
            source_id, data = self._receive()
            self.__my_data = data
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
            self._send(0, self.__my_data)

        self.__barrier()
        if self.__worker_id == 0:
            self.__log('Receivied all data!')
            self.__log('Sorted list content')
            for number in sorted_data_complete:
                self.__log('{}'.format(number))

    def compare_low(self, x):
        minimum = None
        counter = 0
        list_of_received = [i % 1 for i in range(0, self.__data_per_process_size + 1)]
        list_to_send = [i % 1 for i in range(0, self.__data_per_process_size + 1)]

        # Paired process id
        # Calculated by XOR with 1 shifted left x positions
        paired_process_id = self.__worker_id ^ (1 << x)

        # Send the biggest of the list and receive the smallest of the list
        # We need to split it into two parts
        # For the communication to take place
        # Otherwise both processes would want to
        # Send or receive at the same time - be blocked

        # Process with lower id receives first
        if self.__worker_id < paired_process_id:
            #  Receive new minimum of sorted numbers
            source_id, minimum_marshalled = self._receive(paired_process_id)
            minimum = minimum_marshalled[0]
        # Process with higher id sends first
        else:
            # Send my biggest number to paired neighbour
            # ( Last element is the biggest element, because of bitonic sequence)
            # Marshalling because that is how i roll
            self._send(paired_process_id, [self.__my_data[self.__data_per_process_size - 1]])

        # Now it's time for the process with lower id to send
        if self.__worker_id < paired_process_id:
            # Send my biggest number to paired neighbour
            # ( Last element is the biggest element, because of bitonic sequence)
            # Marshalling because that is how i roll
            self._send(paired_process_id, [self.__my_data[self.__data_per_process_size - 1]])
        # This time process with higher id receives
        else:
            #  Receive new minimum of sorted numbers
            source_id, minimum_marshalled = self._receive(paired_process_id)
            minimum = minimum_marshalled[0]

        # Store all values which are bigger than minimum received from paired_process
        for i in range(0, self.__data_per_process_size):
            if self.__my_data[i] > minimum:
                list_to_send[counter + 1] = self.__my_data[i]
                counter += 1
            else:
                # This helps us to save big number of cycles
                break

        # First element in array, will be it's size
        list_to_send[0] = counter

        # Send all values that are greater than minimum, and receive
        # Once again, we need to split it into two parts
        # For the communication to take place
        # Otherwise both processes would want to
        # Send or receive at the same time - be blocked

        # Process with lower id receives first
        if self.__worker_id < paired_process_id:
            #  Receive partition from paired process
            source_id, list_of_received = self._receive(paired_process_id)
        # Process with higher id sends first
        else:
            # Send all elements that are bigger than minimum to
            # The paired process
            self._send(paired_process_id, list_to_send)

        # Now it's time for the process with lower id to send
        if self.__worker_id < paired_process_id:
            # Send all elements that are bigger than minimum to
            # The paired process
            self._send(paired_process_id, list_to_send)
        # This time process with higher id receives
        else:
            #  Receive partition from paired process
            source_id, list_of_received = self._receive(paired_process_id)

        # Take all received values which are smaller than
        # the current biggest element
        for i in range(1, list_of_received[0] + 1):
            if self.__my_data[self.__data_per_process_size - 1] < list_of_received[i]:
                self.__my_data[self.__data_per_process_size - 1] = list_of_received[i]
            else:
                # This helps us to save big number of cycles
                break

        self.__my_data = self.quick_sort(self.__my_data)

    def compare_high(self, x):
        maximum = None
        receiving_counter = 0
        sending_counter = 0
        list_of_received = [i % 1 for i in range(0, self.__data_per_process_size + 1)]
        list_to_send = [i % 1 for i in range(0, self.__data_per_process_size + 1)]

        # Paired process id
        # Calculated by XOR with 1 shifted left x positions
        paired_process_id = self.__worker_id ^ (1 << x)

        # Receive maximum from paired process and send minimum to it
        # We need to split it into two parts
        # For the communication to take place
        # Otherwise both processes would want to
        # Send or receive at the same time - be blocked

        # Process with lower id receives first
        if self.__worker_id < paired_process_id:
            #  Receive new maximum of sorted numbers
            source_id, maximum_marshalled = self._receive(paired_process_id)
            maximum = maximum_marshalled[0]
        # Process with higher id sends first
        else:
            # Send my smallest number to paired neighbour
            # ( Last element is the biggest element, because of bitonic sequence)
            # Marshalling because that is how i roll
            self._send(paired_process_id, [self.__my_data[0]])

        # Now it's time for the process with lower id to send
        if self.__worker_id < paired_process_id:
            # Send my smallest number to paired neighbour
            # ( Last element is the biggest element, because of bitonic sequence)
            # Marshalling because that is how i roll
            self._send(paired_process_id, [self.__my_data[0]])
        # This time process with higher id receives
        else:
            #  Receive new maximum of sorted numbers
            source_id, maximum_marshalled = self._receive(paired_process_id)
            maximum = maximum_marshalled[0]

        # Store all values which are smaller than maximum received from paired_process
        for i in range(0, self.__data_per_process_size):
            if self.__my_data[i] < maximum:
                list_to_send[sending_counter + 1] = self.__my_data[i]
                sending_counter += 1
            else:
                # This helps us to save big number of cycles
                break

        # Send all values that are smaller than maximum, and receive
        # Once again, we need to split it into two parts
        # For the communication to take place
        # Otherwise both processes would want to
        # Send or receive at the same time - be blocked

        # Process with lower id receives first
        if self.__worker_id < paired_process_id:
            #  Receive greater than min from paired process
            source_id, list_of_received = self._receive(paired_process_id)
            receiving_counter = list_of_received[0]
        # Process with higher id sends first
        else:
            # Send all elements that are smaller than maximum to
            # The paired process
            self._send(paired_process_id, list_to_send)

        # Now it's time for the process with lower id to send
        if self.__worker_id < paired_process_id:
            # Send all elements that are smaller than maximum to
            # The paired process
            self._send(paired_process_id, list_to_send)
        # This time process with higher id receives
        else:
            #  Receive greater than min from paired process
            source_id, list_of_received = self._receive(paired_process_id)
            receiving_counter = list_of_received[0]

        # Take values from process which are greater than current minimum
        for i in range(1, receiving_counter + 1):
            if list_of_received[i] > self.__my_data[0]:
                self.__my_data[0] = list_of_received[i]
            else:
                # This helps us to save big number of cycles
                break

        self.__my_data = self.quick_sort(self.__my_data)

    def run(self):
        self.__log('Started.')

        self.handle_data_creation_and_distribution()
        self.__my_data = self.quick_sort(self.__my_data)

        for i in range(0, self.__dimensions):
            j = i
            while j >= 0:
                # (window_id is even AND jth bit of process is 0)
                # OR (window_id is odd AND jth bit of process is 1)
                if (((self.__worker_id >> (i + 1) % 2) == 0) and (((self.__worker_id >> j) % 2) == 0)) or ((((self.__worker_id >> (i + 1)) % 2) != 0) and (((self.__worker_id >> j) % 2) != 0)):
                    self.compare_low(j)
                else:
                    self.compare_high(j)
                j -= 1

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
