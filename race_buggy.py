#!/usr/bin/env python3
"""race_buggy.py - ПРОГРАММА С ГОНКОЙ: 4 потока увеличивают общий счётчик без блокировки."""
import sys
import threading
import time

N_THREADS = 4
N_ITER = 20_000
counter = 0


def worker():
    global counter
    for _ in range(N_ITER):
        tmp = counter       # 1) прочитали
        time.sleep(0)       # здесь поток может быть вытеснен (отдаёт GIL другим потокам)
        counter = tmp + 1   # 2) записали старое значение + 1: чужие инкременты между 1 и 2 потеряны


threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
for t in threads:
    t.start()
for t in threads:
    t.join()

expected = N_THREADS * N_ITER
print(f"ожидалось: {expected}")
print(f"получилось: {counter}")
if counter != expected:
    print(f"ГОНКА ОБНАРУЖЕНА: потеряно {expected - counter} инкрементов")
    sys.exit(1)
print("гонка в этот раз не проявилась (запустите ещё раз)")
