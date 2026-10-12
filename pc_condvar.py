#!/usr/bin/env python3
"""pc_condvar.py - производитель-потребитель: мьютекс + условные переменные."""
import sys
import threading

from pc_common import CAPACITY, RingBuffer, run


class CondBuffer(RingBuffer):
    def __init__(self, violations):
        super().__init__(violations)
        self.mutex = threading.Lock()
        self.not_full = threading.Condition(self.mutex)    # обе переменные привязаны к ОДНОМУ мьютексу
        self.not_empty = threading.Condition(self.mutex)

    def put(self, item):
        with self.not_full:                      # захватывает self.mutex, освобождает при выходе (даже при исключении)
            while self.count == CAPACITY:        # while, а не if: после пробуждения условие надо перепроверить
                self.not_full.wait()
            self._store(item)
            self.not_empty.notify()              # появился элемент - будим одного потребителя

    def get(self):
        with self.not_empty:
            while self.count == 0:
                self.not_empty.wait()
            item = self._take()
            self.not_full.notify()               # освободилось место - будим одного производителя
            return item


if __name__ == "__main__":
    sys.exit(run(CondBuffer, "производитель-потребитель: мьютекс + условные переменные"))
