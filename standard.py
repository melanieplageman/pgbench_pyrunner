import subprocess
import shutil
import os
import psycopg2

pginstall = os.path.expandvars("$PGINSTALL")
pg_ctl = os.path.join(pginstall, "pg_ctl")
pgbench = os.path.join(pginstall, "pgbench")
psql = os.path.join(pginstall, "psql")

def initialize():
    try:
        subprocess.check_call([pg_ctl, "-w", "stop"])
    except subprocess.CalledProcessError as e:
        print(f"Return code {e.returncode} while stopping server")

    shutil.rmtree(os.path.expandvars("$PGDATA"))

    subprocess.check_call(os.path.join(pginstall, "initdb"))

    subprocess.check_call([pg_ctl, "start", "-l", os.path.expandvars("$PGLOG")])

    subprocess.check_call(os.path.join(pginstall, "createdb"))
    conn = psycopg2.connect()
    return conn

def restart(conn):
    conn.close()
    subprocess.check_call([pg_ctl, "restart", "-l", os.path.expandvars("$PGLOG")])
    conn = psycopg2.connect()
    return conn

def pgbench_load(resultsdir):
    with open(os.path.join(resultsdir, 'load_summary'), 'w') as f:
        subprocess.call([pgbench, "-i", "--no-vacuum", "-s", "1"],
                                        stdout=f, stderr=subprocess.STDOUT)

def create_extensions(conn):
    extensions = ['pg_prewarm', 'pg_buffercache', 'pg_visibility',
        'pageinspect', 'pg_walinspect']

    with conn.cursor() as cur:
        for extension in extensions:
            cur.execute(f"CREATE EXTENSION {extension}")
            conn.commit()

def create_pgbench_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX ON pgbench_accounts(abalance);")
        cur.execute("CREATE INDEX ON pgbench_tellers(tbalance);")
        cur.execute("CREATE INDEX ON pgbench_branches(bbalance);")
        conn.commit()

def pgbench_prewarm(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname LIKE 'pgbench%';")
        conn.commit()

def set_gucs(conn):
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

    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        for name, value in gucs.items():
            cur.execute(f"ALTER SYSTEM SET {name} = '{value}';")
    conn.set_session(autocommit=False)

def pgbench_run_and_log(resultsdir, extra_args):
    args = [pgbench, '--progress-timestamp', '--random-seed=0',
    '--no-vacuum', '-M', 'prepared']
    args.extend(extra_args)
    summary = open(os.path.join(resultsdir, 'run_summary'), 'w')
    progress = open(os.path.join(resultsdir, 'run_progress.raw'), 'w')
    subprocess.call(args, stdout=summary, stderr=progress)
    summary.close()
    progress.close()

def reset_stats(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_stat_force_next_flush()")
        cur.execute("SELECT pg_stat_reset_shared('io')")
        cur.execute("SELECT pg_stat_reset_shared('wal')")
        conn.commit()

def pgbench_vacuum(conn):
    pgbench_tables = ['pgbench_accounts', 'pgbench_branches',
                      'pgbench_history', 'pgbench_tellers']
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        for table in pgbench_tables:
            cur.execute(f"VACUUM (VERBOSE) {table}")
    conn.set_session(autocommit=False)

def cleanup(conn):
    conn.close()
