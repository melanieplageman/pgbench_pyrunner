import subprocess
import shutil
import os
import psycopg2

pginstall = os.path.expandvars("$PGINSTALL")
pg_ctl = os.path.join(pginstall, "pg_ctl")
pgbench = os.path.join(pginstall, "pgbench")
psql = os.path.join(pginstall, "psql")

def initdb():
    try:
        subprocess.check_call([pg_ctl, "-w", "stop"])
    except subprocess.CalledProcessError as e:
        print(f"Return code {e.returncode} while stopping server")

    shutil.rmtree(os.path.expandvars("$PGDATA"))

    subprocess.check_call(os.path.join(pginstall, "initdb"))

    subprocess.check_call([pg_ctl, "start", "-l", os.path.expandvars("$PGLOG")])

    subprocess.check_call(os.path.join(pginstall, "createdb"))

def restart():
    subprocess.check_call([pg_ctl, "restart", "-l", os.path.expandvars("$PGLOG")])

def pgbench_load(resultsdir):
    with open(os.path.join(resultsdir, 'load_summary'), 'w') as f:
        subprocess.call([pgbench, "-i", "--no-vacuum", "-s", "1"],
                                        stdout=f, stderr=subprocess.STDOUT)

def create_extensions():
    extensions = ['pg_prewarm', 'pg_buffercache', 'pg_visibility',
        'pageinspect', 'pg_walinspect']
    conn = psycopg2.connect()
    cur = conn.cursor()
    for extension in extensions:
        SQL = f"CREATE EXTENSION {extension}"
        cur.execute(SQL)
    conn.commit()
    cur.close()
    conn.close()

def create_pgbench_indexes():
    conn = psycopg2.connect()
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON pgbench_accounts(abalance);")
    cur.execute("CREATE INDEX ON pgbench_tellers(tbalance);")
    cur.execute("CREATE INDEX ON pgbench_branches(bbalance);")
    conn.commit()
    cur.close()
    conn.close()

def pgbench_prewarm():
    conn = psycopg2.connect()
    cur = conn.cursor()
    cur.execute("SELECT pg_prewarm(oid::regclass), relname FROM pg_class WHERE relname LIKE 'pgbench%';")
    conn.commit()
    cur.close()
    conn.close()

def set_gucs():
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

    conn = psycopg2.connect()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    for name, value in gucs.items():
        SQL = f"ALTER SYSTEM SET {name} = '{value}';"
        cur.execute(SQL)
    cur.close()
    conn.close()

def set_guc(name, value):
    conn = psycopg2.connect()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    SQL = f"ALTER SYSTEM SET {name} = '{value}';"
    cur.execute(SQL)
    cur.close()
    conn.close()

def pgbench_run_and_log(resultsdir, extra_args):
    args = [pgbench, '--progress-timestamp', '--random-seed=0',
    '--no-vacuum', '-M', 'prepared']
    args.extend(extra_args)
    summary = open(os.path.join(resultsdir, 'run_summary'), 'w')
    progress = open(os.path.join(resultsdir, 'run_progress.raw'), 'w')
    subprocess.call(args, stdout=summary, stderr=progress)
    summary.close()
    progress.close()

def reset_stats():
    conn = psycopg2.connect()
    cur = conn.cursor()
    cur.execute("SELECT pg_stat_force_next_flush()")
    cur.execute("SELECT pg_stat_reset_shared('io')")
    cur.execute("SELECT pg_stat_reset_shared('wal')")
    conn.commit()
    cur.close()
    conn.close()

def pgbench_vacuum():
    pgbench_tables = ['pgbench_accounts', 'pgbench_branches',
                      'pgbench_history', 'pgbench_tellers']
    conn = psycopg2.connect()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    for table in pgbench_tables:
        SQL = f"VACUUM (VERBOSE) {table}"
        cur.execute(SQL)
    cur.close()
    conn.close()
