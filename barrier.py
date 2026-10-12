#!/usr/bin/env python3
"""barrier.py - барьер на условной переменной: 4 потока, 3 фазы.
Запуск: python3 barrier.py            - с барьером
        python3 barrier.py --no-barrier - без барьера (показывает, что проверка ловит нарушения)"""
import random
import sys
import threading
import time

N_THREADS = 4
N_PHASES = 3


class Barrier:
    """Многоразовый барьер: последний пришедший поток будит остальных."""

    def __init__(self, parties):
        self.parties = parties
        self.cond = threading.Condition()
        self.waiting = 0
        self.generation = 0      # номер "поколения" - защита от пробуждений не по делу и от обгона

    def wait(self):
        with self.cond:
            gen = self.generation
            self.waiting += 1
            if self.waiting == self.parties:      # я последний
                self.waiting = 0
                self.generation += 1              # открываем барьер
                self.cond.notify_all()
            else:
                while gen == self.generation:     # while: условие перепроверяется после каждого пробуждения
                    self.cond.wait()


def main():
    use_barrier = "--no-barrier" not in sys.argv
    barrier = Barrier(N_THREADS)
    lock = threading.Lock()
    finished = [0] * N_PHASES            # сколько потоков закончило фазу k
    t_first_start = [None] * N_PHASES
    t_last_end = [0.0] * N_PHASES
    violations = []
    t0 = time.monotonic()

    def worker(i):
        for ph in range(N_PHASES):
            with lock:
                # ИНВАРИАНТ: начать фазу ph можно, только когда ВСЕ потоки закончили фазу ph-1
                if ph > 0 and finished[ph - 1] != N_THREADS:
                    violations.append(f"поток {i} начал фазу {ph + 1}, а фазу {ph} закончили только "
                                      f"{finished[ph - 1]} из {N_THREADS}")
                if t_first_start[ph] is None:
                    t_first_start[ph] = time.monotonic() - t0
            time.sleep(random.uniform(0.01, 0.1))     # "работа" фазы вне критической секции
            with lock:
                finished[ph] += 1
                t_last_end[ph] = max(t_last_end[ph], time.monotonic() - t0)
            if use_barrier:
                barrier.wait()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)

    print(f"=== барьер: {'ВКЛЮЧЁН' if use_barrier else 'ОТКЛЮЧЁН'}, потоков {N_THREADS}, фаз {N_PHASES} ===")
    for ph in range(N_PHASES):
        print(f"фаза {ph + 1}: первый старт +{t_first_start[ph] * 1000:6.1f} мс, "
              f"последнее завершение +{t_last_end[ph] * 1000:6.1f} мс")
    alive = [t for t in threads if t.is_alive()]
    for ph in range(N_PHASES - 1):
        if t_first_start[ph + 1] < t_last_end[ph]:
            print(f"  фаза {ph + 2} началась раньше, чем закончилась фаза {ph + 1}")
    print(f"нарушений порядка фаз: {len(violations)}")
    for text in violations[:5]:
        print("  НАРУШЕНИЕ:", text)
    ok = not violations and not alive and all(f == N_THREADS for f in finished)
    print("ИТОГ:", "ВСЕ ФАЗЫ ВЫПОЛНЕНЫ В ПРАВИЛЬНОМ ПОРЯДКЕ" if ok else "ЕСТЬ НАРУШЕНИЯ")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
