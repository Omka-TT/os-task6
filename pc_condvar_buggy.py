#!/usr/bin/env python3
"""pc_condvar_buggy.py - ТА ЖЕ программа, но while заменён на if: демонстрация потерянного пробуждения."""
import sys
import threading

from pc_common import CAPACITY, RingBuffer, run


class BuggyCondBuffer(RingBuffer):
    def __init__(self, violations):
        super().__init__(violations)
        self.mutex = threading.Lock()
        self.not_full = threading.Condition(self.mutex)
        self.not_empty = threading.Condition(self.mutex)

    def put(self, item):
        with self.not_full:
            if self.count == CAPACITY:           # ОШИБКА: if вместо while
                self.not_full.wait()
            self._store(item)
            self.not_empty.notify()

    def get(self):
        with self.not_empty:
            if self.count == 0:                  # ОШИБКА: if вместо while
                self.not_empty.wait()
            item = self._take()
            self.not_full.notify()
            return item


if __name__ == "__main__":
    sys.exit(run(BuggyCondBuffer, "ОШИБОЧНАЯ версия: if вместо while", timeout=20))
