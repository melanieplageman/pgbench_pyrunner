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
            self.emit(cursor.fetchone())

class VacuumFrzCollector(SeriesCollector):
    def __init__(self, tables, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tables = tables

    def before_run(self):
        subprocess.check_call(['psql', '-f', '/tmp/frz.sql'])

    def after_run(self):
        self.connection = psycopg.connect()
        with self.connection.cursor() as cursor:
            for table in self.tables:
                q = f"""
                SELECT * FROM freeze_errs('{table}');
                SELECT * FROM vacstats('{table}');
                SELECT * FROM av_efficacy('{table}');
                """
                cursor.execute(q)
                while cursor.nextset():
                    self.emit(cursor.fetchall())
