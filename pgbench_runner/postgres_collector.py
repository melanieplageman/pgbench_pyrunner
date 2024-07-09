from pgbench_runner import ScalarCollector, SeriesCollector, Signaler, IntervalCollector
import psycopg
import subprocess

class BenchRunCollector(IntervalCollector):
    def after_restart(self):
        self.connection = psycopg.connect()

    def before_run(self):
        self.thread.start()

    def after_run(self):
        self.stop.set()

    def after_cleanup(self):
        self.connection.close()
        super().cleanup()

class QueryBenchRunCollector(BenchRunCollector):
    def __init__(self, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query

    def before_run(self):
        with self.connection.cursor() as cursor:
            cursor.execute(self.query)
            self.emit([c.name for c in cursor.description])

        super().before_run()

    def invoke(self):
        with self.connection.cursor() as cursor:
            cursor.execute(self.query)
            ts, *rest = cursor.fetchone()
            self.emit([ts.isoformat(), *rest])


class WALCollector(QueryBenchRunCollector):
    def __init__(self, *args, **kwargs):
        query = "SELECT now() AS ts, * FROM pg_stat_wal"
        super().__init__(query, *args, **kwargs)

class PgStatIOCollector(QueryBenchRunCollector):
    def __init__(self, *args, **kwargs):
        query = "SELECT now() AS ts, * FROM pg_stat_io"
        super().__init__(query, *args, **kwargs)

class PgStatAllTablesCollector(QueryBenchRunCollector):
    def __init__(self, *args, **kwargs):
        query = "SELECT now() AS ts, * FROM pg_stat_all_tables"
        super().__init__(query, *args, **kwargs)

class RelfrozenxidCollector(QueryBenchRunCollector):
    def __init__(self, *args, **kwargs):
        query = "SELECT now() AS ts, relfrozenxid FROM pg_class"
        super().__init__(query, *args, **kwargs)
