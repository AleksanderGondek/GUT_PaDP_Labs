__author__ = 'moorglade, Alex'

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
    parser.add_argument('n_workers', help='number of workers in the distributed system', type=int)
    parser.add_argument('delay_connect', help='network connection delay [s]', type=float)
    parser.add_argument('delay_transmit', help='network transmission delay [s]', type=float)
    parser.add_argument('delay_process', help='processing delay [s]', type=float)

    return parser.parse_args()


class DistributedSystem(object):
    def __init__(self, configuration):
        object.__init__(self)

        network = Network(configuration)
        self.__workers = [
            Worker(worker_id, configuration, network.get_endpoint(worker_id))
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


class Network(object):
    def __init__(self, configuration):
        object.__init__(self)
        channels = [NetworkChannel(configuration) for _ in range(configuration.n_workers)]
        self.__endpoints = [NetworkEndpoint(channel_id, channels) for channel_id in range(configuration.n_workers)]

    def get_endpoint(self, index):
        return self.__endpoints[index]


class NetworkChannel(object):
    def __init__(self, configuration):
        self.__configuration = configuration

        self.__queue = multiprocessing.Queue(maxsize=1)
        self.__enter_lock = multiprocessing.Lock()
        self.__exit_lock = multiprocessing.Lock()
        self.__enter_lock.acquire()
        self.__exit_lock.acquire()

    def send(self, data):
        self.__enter_lock.acquire()
        time.sleep(self.__configuration.delay_connect)
        self.__queue.put(data)
        time.sleep(len(data[1]) * self.__configuration.delay_transmit)
        self.__exit_lock.release()

    def receive(self):
        self.__enter_lock.release()
        data = self.__queue.get()
        self.__exit_lock.acquire()
        return data


class NetworkEndpoint(object):
    def __init__(self, channel_id, channels):
        self.__channels = channels
        self.__my_id = channel_id
        self.__my_channel = self.__channels[self.__my_id]

    def send(self, destination_id, data):
        if destination_id == self.__my_id:
            raise RuntimeError('Worker {} tried to send data to itself.'.format(self.__my_id))

        self.__channels[destination_id].send((self.__my_id, data))

    def receive(self):
        return self.__my_channel.receive()


class Worker(multiprocessing.Process):
    def __init__(self, worker_id, configuration, network_endpoint):
        multiprocessing.Process.__init__(self)

        self.__worker_id = worker_id
        self.__configuration = configuration
        self.__network_endpoint = network_endpoint
        self.__my_generation = None

    @property
    def __n_workers(self):
        return self.__configuration.n_workers

    def _send(self, worker_id, data):
        self.__network_endpoint.send(worker_id, data)

    def _receive(self):
        data = self.__network_endpoint.receive()
        time.sleep(len(data[1]) * self.__configuration.delay_process)
        return data

    @staticmethod
    def __generate_random_data(length):
        return [random.randint(-2048, 2048) for _ in range(length)]

    # My methods
    def __get_number_of_generations_needed(self):
        generations_needed = math.log(self.__n_workers, 2.0)
        generations_needed = math.floor(generations_needed)

        return int(generations_needed)

    def __get_recipient_index(self, generation):
        index = self.__worker_id + int(math.pow(2.0, generation))
        if index >= self.__n_workers:
            return -1

        return index

    def __which_generation_am_i(self, sender_id):
        # Calculate in which gen we received data
        difference = math.fabs(self.__worker_id - sender_id)
        generation = math.log(difference, 2.0)
        generation = math.floor(generation)  # Just to be sure
        generation = int(generation)

        return generation + 1

    def __custom_send(self, data, recipient_index):
        if recipient_index < 0:
            return

        self._send(recipient_index, data)

    def __handle_root(self, last_generation_index):
        if self.__worker_id != 0:
            return

        self.__my_generation = 0
        data = Worker.__generate_random_data(16)
        while self.__my_generation <= last_generation_index:
            recipient_index = self.__get_recipient_index(self.__my_generation)
            self.__custom_send(data, recipient_index)
            self.__my_generation += 1

    def __handle_leaves(self, last_generation_index):
        if self.__worker_id == 0:
            return

        # receive is BLOCKING
        # worker will start doing code embed here WHEN AND ONLY WHEN HE WILL RECEIVE SOMETHING
        source_id, data = self._receive()

        # print 'Worker {} received data from worker {}: {}'.format(self.__worker_id, source_id, data)
        print '{} -> {} - Data: {}'.format(source_id, self.__worker_id, data)

        self.__my_generation = self.__which_generation_am_i(source_id)
        while self.__my_generation <= last_generation_index:
            recipient_index = self.__get_recipient_index(self.__my_generation)
            self.__custom_send(data, recipient_index)
            self.__my_generation += 1

    def run(self):
        generations_needed = self.__get_number_of_generations_needed()
        self.__handle_root(generations_needed)
        self.__handle_leaves(generations_needed)


def main():
    random.seed()
    configuration = _parse_args()
    system = DistributedSystem(configuration)
    system.run()


if __name__ == '__main__':
    sys.exit(main())
