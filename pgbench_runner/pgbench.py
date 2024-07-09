import os
import subprocess
import psycopg


class Pgbench:
    def __init__(self, tables=[]):
        self.tables = tables

    def prewarm(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            for table in self.tables:
                cur.execute(f"SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname = '{table}';")
            conn.commit()
        conn.close()

    def run_and_log(self, name, *args):
        pass

    def vacuum(self):
        conn = psycopg.connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in self.tables:
                cur.execute(f"VACUUM (VERBOSE) {table}")
        conn.close()


class PgbenchDefault(Pgbench):
    def __init__(self, tables=[]):
        super().__init__(tables = [
            'pgbench_accounts',
            'pgbench_branches',
            'pgbench_history',
            'pgbench_tellers',
        ])

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

    def run_and_log(self, name, *args):
        with open(name + '_run_summary', 'w') as summary:
            with open(name + '_run_progress.raw', 'w') as progress:
                subprocess.call([
                    'pgbench',
                    '--progress-timestamp',
                    '--random-seed=0',
                    '--no-vacuum', '-M', 'prepared',
                    *args], stdout=summary, stderr=progress)



class PgbenchSwitchInsertUpdate(Pgbench):
    def __init__(self):
        super().__init__(tables = ['insert_update_shift'])
        self.run_counter = 0

    def load(self, fillfactor=100):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE insert_update_shift(
                id SERIAL,
                description TEXT,
                quantity INT,
                itime TIMESTAMPTZ,
                client_id INT)
                WITH (fillfactor = {fillfactor});
            """)
            conn.commit()
        conn.close()

    def create_indexes(self):
        conn = psycopg.connect()
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX ON insert_update_shift(id);")
            cur.execute("CREATE INDEX ON insert_update_shift(quantity);")
            cur.execute("CREATE INDEX ON insert_update_shift(client_id);")
            cur.execute("CREATE INDEX ON insert_update_shift(itime);")
            conn.commit()
        conn.close()

    def run_and_log(self, name, *args):
        if self.run_counter % 2 == 0:
            run_script = """
                BEGIN;
                INSERT INTO insert_update_shift(
                description, quantity, itime, client_id)
                    SELECT repeat(i::TEXT, 2 + i), 1,
                    CURRENT_TIMESTAMP, :client_id FROM generate_series(1,5)i;
                END;
                """
        else:
            conn = psycopg.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM insert_update_shift")
                nrows = cur.fetchone()[0]
            run_script = f"""
                \set target random(1, {nrows})
                UPDATE insert_update_shift
                    SET quantity = quantity + 1
                    WHERE id = :target AND client_id = :client_id;
                """

        self.run_counter += 1

        with open(name + '_run_summary', 'w') as summary:
            with open(name + '_run_progress.raw', 'w') as progress:
                subprocess.run([
                    'pgbench',
                    '--progress-timestamp',
                    '--random-seed=0',
                    '--no-vacuum', '-M', 'prepared', '-f-',
                    *args], stdout=summary, stderr=progress,
                                input=run_script, text=True)

