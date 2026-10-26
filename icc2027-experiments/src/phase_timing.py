"""Monotonic exclusive phase accounting; envelopes must be reported separately."""
from collections import defaultdict
from contextlib import contextmanager
from time import perf_counter


class PhaseTiming:
    def __init__(self, clock=perf_counter):
        self.clock = clock
        self.seconds = defaultdict(float)
        self.active = None

    @contextmanager
    def measure(self, name):
        if self.active is not None:
            raise RuntimeError("Exclusive timing phases cannot overlap")
        self.active = name
        started = self.clock()
        try:
            yield
        finally:
            self.seconds[name] += self.clock() - started
            self.active = None

    def iterate(self, iterable, name):
        iterator = iter(iterable)
        while True:
            with self.measure(name):
                try:
                    item = next(iterator)
                except StopIteration:
                    return
            yield item

    def snapshot(self, total):
        accounted = sum(self.seconds.values())
        if accounted > total + 1e-6:
            raise RuntimeError("Exclusive phase accounting exceeds elapsed wall time")
        return {**dict(self.seconds), "unattributed_seconds": max(0.0, total - accounted)}
