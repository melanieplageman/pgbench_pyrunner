from pgbench_runner import ScalarCollector, SeriesCollector, Signaler, IntervalCollector
from time import sleep


class TestScalarCollector(ScalarCollector):
    def before_step_1(self):
        self.emit("THIS IS A METRIC")

class TestSeriesCollector(SeriesCollector):
    def before_step_1(self):
        self.emit("THIS IS A SERIES METRIC")

    def after_step_2(self):
        self.emit("THIS IS AFTER STEP 2")

class TestIntervalCollector(IntervalCollector):
    def before_step_3(self):
        self.thread.start()

    def invoke(self):
        self.emit("This is before step 3 metric")


interval_collector = TestIntervalCollector(1, 'baz')
collectors = [TestSeriesCollector('foo'), TestScalarCollector('bar'),
              interval_collector]

signaler = Signaler(collectors)
with signaler.signal("step_1"):
    print("Step 1")

with signaler.signal("step_2"):
    print("Step 2")

with signaler.signal("step_3"):
    print("Step 3")

sleep(3)
interval_collector.stop.set()
for collector in collectors:
    collector.cleanup()
