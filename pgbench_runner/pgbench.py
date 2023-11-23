import os
import subprocess
import psycopg


class Pgbench:
    pgbench_tables = [
        'pgbench_accounts',
        'pgbench_branches',
        'pgbench_history',
        'pgbench_tellers',
    ]

    def pgbench_load(self):
        with open('load_summary', 'w') as f:
            subprocess.call(['pgbench', "-i", "--no-vacuum", "-s", "1"],
                            stdout=f, stderr=subprocess.STDOUT)

    def create_pgbench_indexes(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX ON pgbench_accounts(abalance);")
            cur.execute("CREATE INDEX ON pgbench_tellers(tbalance);")
            cur.execute("CREATE INDEX ON pgbench_branches(bbalance);")
            conn.commit()
        conn.close()

    def pgbench_prewarm(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname LIKE 'pgbench%';")
            conn.commit()
        conn.close()

    def pgbench_run_and_log(self, name, *args):
        with open(name + '_run_summary', 'w') as summary:
            with open(name + '_run_progress.raw', 'w') as progress:
                subprocess.call([
                    'pgbench', '--progress-timestamp',
                    '--random-seed=0',
                    '--no-vacuum', '-M', 'prepared',
                    *args], stdout=summary, stderr=progress)

    def pgbench_vacuum(self):
        conn = psycopg.connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in self.pgbench_tables:
                cur.execute(f"VACUUM (VERBOSE) {table}")
        conn.close()
