#!/usr/bin/env python3
"""pc_semaphores.py - производитель-потребитель: два семафора + мьютекс."""
import sys
import threading

from pc_common import CAPACITY, RingBuffer, run


class SemBuffer(RingBuffer):
    def __init__(self, violations):
        super().__init__(violations)
        self.mutex = threading.Lock()                 # защищает сам буфер (head/tail/count/slots)
        self.empty = threading.Semaphore(CAPACITY)    # сколько свободных мест
        self.full = threading.Semaphore(0)            # сколько занятых мест

    def put(self, item):
        self.empty.acquire()          # ждём свободное место (ВНЕ мьютекса, иначе взаимная блокировка)
        with self.mutex:              # критическая секция: только манипуляция с буфером
            self._store(item)
        self.full.release()           # сообщаем: появился элемент

    def get(self):
        self.full.acquire()           # ждём элемент
        with self.mutex:
            item = self._take()
        self.empty.release()          # сообщаем: освободилось место
        return item


if __name__ == "__main__":
    sys.exit(run(SemBuffer, "производитель-потребитель: семафоры + мьютекс"))
