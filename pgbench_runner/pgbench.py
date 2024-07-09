import os
import subprocess
import psycopg


class Pgbench:
    def prewarm(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            for table in self.tables:
                cur.execute(f"SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname = '{table}';")
            conn.commit()
        conn.close()

    def run_and_log(self, name, *args):
        with open(name + '_run_summary', 'w') as summary:
            with open(name + '_run_progress.raw', 'w') as progress:
                subprocess.call([
                    'pgbench', '--progress-timestamp',
                    '--random-seed=0',
                    '--no-vacuum', '-M', 'prepared',
                    *args], stdout=summary, stderr=progress)

    def vacuum(self):
        conn = psycopg.connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in self.tables:
                cur.execute(f"VACUUM (VERBOSE) {table}")
        conn.close()

class PgbenchDefault(Pgbench):
    tables = [
        'pgbench_accounts',
        'pgbench_branches',
        'pgbench_history',
        'pgbench_tellers',
    ]

    def load(self):
        with open('load_summary', 'w') as f:
            subprocess.call(['pgbench', "-i", "--no-vacuum", "-s", "1"],
                            stdout=f, stderr=subprocess.STDOUT)

    def create_indexes(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX ON pgbench_accounts(abalance);")
            cur.execute("CREATE INDEX ON pgbench_tellers(tbalance);")
            cur.execute("CREATE INDEX ON pgbench_branches(bbalance);")
            conn.commit()
        conn.close()

