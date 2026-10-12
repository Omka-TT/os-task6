#!/usr/bin/env python3
"""race_fixed.py - ИСПРАВЛЕННАЯ версия: чтение и запись счётчика защищены мьютексом."""
import sys
import threading
import time

N_THREADS = 4
N_ITER = 20_000
counter = 0
lock = threading.Lock()


def worker():
    global counter
    for _ in range(N_ITER):
        with lock:              # критическая секция: только чтение+запись, ничего лишнего
            tmp = counter
            time.sleep(0)       # то же окно, но теперь внутри блокировки: другие потоки ждут
            counter = tmp + 1


threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
for t in threads:
    t.start()
for t in threads:
    t.join()

expected = N_THREADS * N_ITER
print(f"ожидалось: {expected}")
print(f"получилось: {counter}")
if counter != expected:
    print("ОШИБКА: значение неверно")
    sys.exit(1)
print("OK: потерь нет")
