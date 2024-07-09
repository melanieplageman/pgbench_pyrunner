import os
import tempfile
import datetime

from pgbench_runner import *

root = '/home/melanieplageman/code'
workload = 'A'
algorithm = '3'

workloaddir =  os.path.join(root, workload, algorithm)
os.makedirs(workloaddir, exist_ok=True)
resultsdir = os.path.join(workloaddir, str(datetime.datetime.now()))
os.makedirs(resultsdir)
os.chdir(resultsdir)

runtime = 20
report_sample_interval = 2

postgres = Postgres()
pgbench = Pgbench()

wal_collector = WALCollector('pgstatwal.raw')
pgstatio_collector = PgStatIOCollector('pgstatio.raw')

collectors = [wal_collector, pgstatio_collector]
signaler = Signaler(collectors)

with signaler.signal("initialize"):
    postgres.initialize()
    pgbench.pgbench_load()
    postgres.set_gucs(opp_freeze_algo=3)
    postgres.create_extensions()
    pgbench.create_pgbench_indexes()

with signaler.signal("restart"):
    postgres.restart()
    pgbench.pgbench_prewarm()

postgres.reset_stats()

os.mkdir('execution_reports')

with signaler.signal("run"):
    common_args = ['-T', str(runtime), '-P', str(report_sample_interval)]

    write_args = common_args + [
        '-c', '16',
        '-j', '16',
        '--builtin=simple-update',
    ]

    pgbench.pgbench_run_and_log('write', *write_args)

postgres.reset_stats()

with signaler.signal("vacuum"):
    pgbench.pgbench_vacuum()

with signaler.signal("cleanup"):
    pass
