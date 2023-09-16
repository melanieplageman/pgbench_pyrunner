from contextlib import contextmanager


class Signaler:
    def __init__(self, collectors):
        self.collectors = collectors

    @contextmanager
    def signal(self, name, *args, **kwargs):
        for collector in self.collectors:
            f = getattr(collector, "before_" + name, None)
            if f is not None and callable(f):
                f(*args, **kwargs)

        yield

        for collector in self.collectors:
            f = getattr(collector, "after_" + name, None)
            if f is not None and callable(f):
                f(*args, **kwargs)
