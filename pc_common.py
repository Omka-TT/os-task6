#!/usr/bin/env python3
"""pc_common.py - общий код для "производитель-потребитель": кольцевой буфер с проверкой
инвариантов, запуск потоков и итоговая проверка корректности."""
import logging
import random
import threading
import time
from collections import Counter

CAPACITY = 5       # размер кольцевого буфера
N_PRODUCERS = 3
N_CONSUMERS = 2
N_ITEMS = 1000

SENTINEL = object()  # метка "работа закончена": по одной на каждого потребителя
EMPTY = object()     # метка пустого слота буфера

log = logging.getLogger("pc")


class Violations:
    """Потокобезопасный счётчик нарушений. Первые 10 пишет в лог, остальные только считает."""

    def __init__(self):
        self._lock = threading.Lock()
        self.count = 0
        self.first = []

    def add(self, text):
        with self._lock:
            self.count += 1
            if self.count <= 10:
                self.first.append(text)
                log.error("НАРУШЕНИЕ: %s", text)


class RingBuffer:
    """Кольцевой буфер БЕЗ синхронизации. Блокировки добавляют наследники (put/get).
    Инварианты проверяются здесь, внутри критической секции:
      - запись только в свободный слот и только при count < CAPACITY;
      - чтение только из занятого слота и только при count > 0."""

    def __init__(self, violations):
        self.slots = [EMPTY] * CAPACITY
        self.head = 0      # откуда читаем
        self.tail = 0      # куда пишем
        self.count = 0
        self.max_count = 0
        self.v = violations

    def _store(self, item):
        if self.count >= CAPACITY or self.slots[self.tail] is not EMPTY:
            self.v.add(f"переполнение: запись {item!r} при count={self.count}")
            return False   # элемент потерян - это обнаружит итоговая проверка
        self.slots[self.tail] = item
        self.tail = (self.tail + 1) % CAPACITY
        self.count += 1
        self.max_count = max(self.max_count, self.count)
        return True

    def _take(self):
        if self.count <= 0 or self.slots[self.head] is EMPTY:
            self.v.add(f"чтение из пустого буфера при count={self.count}")
            return EMPTY
        item = self.slots[self.head]
        self.slots[self.head] = EMPTY
        self.head = (self.head + 1) % CAPACITY
        self.count -= 1
        return item


def _producer(buf, items, jitter):
    for it in items:
        time.sleep(random.uniform(0, jitter))   # имитация работы вне критической секции
        buf.put(it)


def _consumer(buf, out, jitter):
    while True:
        item = buf.get()
        if item is SENTINEL:
            return
        if item is EMPTY:      # нарушение уже залогировано в _take
            continue
        out.append(item)       # у каждого потребителя свой список - гонки нет
        time.sleep(random.uniform(0, jitter))


def run(make_buffer, title, jitter=0.0002, timeout=60):
    """Запускает 3 производителей и 2 потребителей, проверяет инварианты. Возвращает код выхода."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(f"=== {title} ===")
    print(f"буфер: {CAPACITY}, производителей: {N_PRODUCERS}, потребителей: {N_CONSUMERS}, элементов: {N_ITEMS}")
    v = Violations()
    buf = make_buffer(v)

    chunks = [list(range(i, N_ITEMS, N_PRODUCERS)) for i in range(N_PRODUCERS)]  # все 1000 без повторов
    results = [[] for _ in range(N_CONSUMERS)]
    producers = [threading.Thread(target=_producer, args=(buf, chunks[i], jitter),
                                  name=f"prod-{i}", daemon=True) for i in range(N_PRODUCERS)]
    consumers = [threading.Thread(target=_consumer, args=(buf, results[i], jitter),
                                  name=f"cons-{i}", daemon=True) for i in range(N_CONSUMERS)]
    t0 = time.monotonic()
    for t in consumers + producers:
        t.start()

    deadline = t0 + timeout
    for t in producers:
        t.join(max(0, deadline - time.monotonic()))
    if not any(t.is_alive() for t in producers):
        for _ in range(N_CONSUMERS):
            buf.put(SENTINEL)          # по одной метке остановки на каждого потребителя
    for t in consumers:
        t.join(max(0, deadline - time.monotonic()))
    elapsed = time.monotonic() - t0

    alive = [t.name for t in producers + consumers if t.is_alive()]
    consumed = [x for r in results for x in r]
    cnt = Counter(consumed)
    expected = set(range(N_ITEMS))
    missing = sorted(expected - set(cnt))
    dups = sorted(k for k, c in cnt.items() if c > 1)
    extra = sorted(set(cnt) - expected)

    checks = [
        ("все 1000 элементов потреблены", not missing, f"потеряно: {len(missing)}"),
        ("каждый элемент потреблён ровно один раз", not dups, f"дублей: {len(dups)}"),
        ("нет посторонних элементов", not extra, f"лишних: {len(extra)}"),
        (f"инварианты буфера 0 <= count <= {CAPACITY} (нет переполнения и чтения из пустого)",
         v.count == 0, f"нарушений: {v.count}"),
        ("буфер в конце пуст", buf.count == 0, f"count={buf.count}"),
        ("все потоки завершились (нет взаимной блокировки)", not alive, f"зависли: {alive}"),
    ]
    print(f"потреблено: {len(consumed)} (по потребителям: {[len(r) for r in results]}), время: {elapsed:.2f} c")
    print(f"максимальное заполнение буфера: {buf.max_count} из {CAPACITY}")
    ok = True
    for name, passed, detail in checks:
        print(f"  [{'OK' if passed else 'FAIL'}] {name}" + ("" if passed else f" - {detail}"))
        ok = ok and passed
    print("ИТОГ:", "ВСЕ ИНВАРИАНТЫ ВЫПОЛНЕНЫ" if ok else "ЕСТЬ НАРУШЕНИЯ")
    return 0 if ok else 1
