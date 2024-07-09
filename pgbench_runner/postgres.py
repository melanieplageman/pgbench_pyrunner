import subprocess
import shutil
import os
import psycopg

class Postgres:
    def __init__(self):
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = psycopg.connect(autocommit=True)
        return self._connection

    def initialize(self):
        returncode = subprocess.call(['pg_ctl', "-w", "stop"])
        print(f"Return code {returncode} while stopping server")

        shutil.rmtree(os.environ["PGDATA"])

        subprocess.check_call("initdb")

        subprocess.check_call(['pg_ctl', "start", "-l", os.environ["PGLOG"]])

        subprocess.check_call("createdb")

    def restart(self):
        if self._connection is not None:
            self._connection.close()
        self._connection = None

        logfile = os.environ["PGLOG"]
        os.truncate(logfile, 0)
        subprocess.check_call(['pg_ctl', "restart", "-l", os.environ["PGLOG"]])

    def create_extensions(self, *args):
        extensions = [
            'pg_prewarm', 'pg_buffercache', 'pg_visibility',
            'pageinspect', 'pg_walinspect', *args,
        ]

        with self.connection.cursor() as cur:
            for extension in extensions:
                cur.execute(f"CREATE EXTENSION {extension}")

    def set_gucs(self, **kwargs):
        gucs = {
            'shared_buffers' : '8GB',
            'log_checkpoints': 'on',
            'track_io_timing': 'on',
            'track_wal_io_timing': 'on',
            'log_autovacuum_min_duration': 0,
            'maintenance_work_mem': '1GB',
            'autovacuum_naptime': 10,
            'max_wal_size': '150GB',
            'min_wal_size': '150GB',
            **kwargs,
        }

        with self.connection.cursor() as cur:
            for name, value in gucs.items():
                cur.execute(f"ALTER SYSTEM SET {name} = '{value}';")

    def reset_stats(self):
        with self.connection.cursor() as cur:
            cur.execute("SELECT pg_stat_force_next_flush()")
            cur.execute("SELECT pg_stat_reset_shared('io')")
            cur.execute("SELECT pg_stat_reset_shared('wal')")

    def force_flush_stats(self):
        with self.connection.cursor() as cur:
            cur.execute("SELECT pg_stat_force_next_flush()")
