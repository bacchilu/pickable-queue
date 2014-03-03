#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
pickeble_queue.py

Estendo la coda in modo fa poterne serializzare sotto forma di pickle il
contenuto.

Luca Bacchi <bacchilu@gmail.com> - http://www.lucabacchi.it
"""

import pickle
import collections
import threading


class PickebleQueue(object):

    """
    http://hg.python.org/cpython/file/2.7/Lib/Queue.py
    """

    def __init__(self):
        self.queue = collections.deque()
        try:
            with open('pendings.pik', 'rb') as fp:
                self.queue = pickle.load(fp)
        except IOError:
            pass

        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)

    def put(self, item):
        self.not_full.acquire()
        try:
            self.queue.append(item)
            self.not_empty.notify()
        finally:
            self.not_full.release()

    def get(self):
        self.not_empty.acquire()
        try:
            while not len(self.queue):
                self.not_empty.wait()
            return self.queue.popleft()
        finally:
            self.not_empty.release()

    def qsize(self):
        self.mutex.acquire()
        try:
            return len(self.queue)
        finally:
            self.mutex.release()

    def pickle(self):
        self.mutex.acquire()
        try:
            with open('pendings.pik', 'wb') as fp:
                pickle.dump(self.queue, fp)
        finally:
            self.mutex.release()


