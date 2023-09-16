from pgbench_runner import ScalarCollector, SeriesCollector, Signaler, IntervalCollector
import psycopg

class PgbenchRunCollector(IntervalCollector):
    def after_restart(self):
        self.connection = psycopg.connect()

    def before_run(self):
        self.thread.start()

    def after_run(self):
        self.stop.set()

    def after_cleanup(self):
        self.connection.close()
        super().cleanup()


class WALCollector(PgbenchRunCollector):
    def invoke(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT NOW() AS ts, * FROM pg_stat_wal")
            self.emit(cursor.fetchone())
