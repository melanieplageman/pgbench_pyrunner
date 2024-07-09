from pgbench_runner import ScalarCollector, SeriesCollector, Signaler, IntervalCollector
import psycopg
import subprocess

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

class PgStatIOCollector(PgbenchRunCollector):
    def invoke(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT NOW() AS ts, * FROM pg_stat_io")
            ts, *rest = cursor.fetchone()
            self.emit(ts.isoformat() + "," + ",".join(list(rest)))

class PgStatAllTablesCollector(PgbenchRunCollector):
    def before_run(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT now() AS ts, * FROM pg_stat_all_tables")
            self.emit(",".join([c.name for c in cursor.description]))
        super().before_run()

    def invoke(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT now() AS ts, * FROM pg_stat_all_tables")
            ts, *rest = cursor.fetchone()
            self.emit(f"{ts.isoformat()}," + ",".join(list(str(item) for item in rest)))

class RelfrozenxidCollector(PgbenchRunCollector):
    def invoke(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT now() AS ts, relfrozenxid FROM pg_class")
            ts, relfrozenxid = cursor.fetchone()
            self.emit(f"{ts.isoformat()},{relfrozenxid}")
