import subprocess
import shutil
import os
import psycopg


class Postgres:
    def __init__(self):
        self.connection = None
        self._pginstall = None
        self._pg_ctl = None

    @property
    def pginstall(self):
        if self._pginstall is None:
            self._pginstall = os.path.expandvars("$PGINSTALL")
        return self._pginstall

    
    @property
    def pg_ctl(self):
        if self._pg_ctl is None:
            self._pg_ctl = os.path.join(self.pginstall, "pg_ctl")
        return self._pg_ctl

    def initialize(self):
        try:
            subprocess.check_call([self.pg_ctl, "-w", "stop"])
        except subprocess.CalledProcessError as e:
            print(f"Return code {e.returncode} while stopping server")

        shutil.rmtree(os.path.expandvars("$PGDATA"))

        subprocess.check_call(os.path.join(self.pginstall, "initdb"))

        subprocess.check_call([self.pg_ctl, "start", "-l", os.path.expandvars("$PGLOG")])

        subprocess.check_call(os.path.join(self.pginstall, "createdb"))
        self.connection = psycopg.connect()

    def restart(self):
        self.connection.close()
        subprocess.check_call([self.pg_ctl, "restart", "-l", os.path.expandvars("$PGLOG")])
        self.connection = psycopg.connect()

    def create_extensions(self):
        extensions = ['pg_prewarm', 'pg_buffercache', 'pg_visibility',
            'pageinspect', 'pg_walinspect']

        with self.connection.cursor() as cur:
            for extension in extensions:
                cur.execute(f"CREATE EXTENSION {extension}")
                self.connection.commit()

    def set_gucs(self):
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
                }

        self.connection.autocommit=True
        with self.connection.cursor() as cur:
            for name, value in gucs.items():
                cur.execute(f"ALTER SYSTEM SET {name} = '{value}';")
        self.connection.autocommit=False

    def reset_stats(self):
        with self.connection.cursor() as cur:
            cur.execute("SELECT pg_stat_force_next_flush()")
            cur.execute("SELECT pg_stat_reset_shared('io')")
            cur.execute("SELECT pg_stat_reset_shared('wal')")
            self.connection.commit()

    def cleanup(self):
        self.connection.close()
