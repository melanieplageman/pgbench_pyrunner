from threading import Thread, Event
import csv

class Collector:
    def __init__(self, name):
        self.name = name
        self.output_file = open(name, 'w')

    def emit(self, data):
        self.output_file.write(str(data))

    def cleanup(self):
        self.output_file.close()

class CSVCollector(Collector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writer= csv.writer(self.output_file)

    def emit(self, data):
        self.writer.writerow(data)


class ScalarCollector(Collector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def emit(self, *args, **kwargs):
        assert self.done is False
        super().emit(*args, **kwargs)
        self.done = True


class SeriesCollector(CSVCollector):
    pass


class IntervalCollector(SeriesCollector):
    def __init__(self, *args, interval=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval: float = interval
        self.stop = Event()
        self.thread = Thread(target=self.invoke_loop, daemon=True)

    def invoke(self):
        pass

    def invoke_loop(self):
        while True:
            self.invoke()
            self.output_file.flush()

            if self.stop.wait(timeout=self.interval) is True:
                return
