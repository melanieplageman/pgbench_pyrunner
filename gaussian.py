import os
import psycopg
from pgbench_runner import Signaler
from pgbench_runner import ScalarCollector, SeriesCollector, IntervalCollector, WALCollector
from pgbench_runner import Postgres, Pgbench

resultsdir = '/tmp/pgresults'

runtime = 10
report_sample_interval = 2
read_log_prefix = os.path.join(resultsdir, 'execution_reports',
                               'pgbench_log_read')

extra_args_both = ['-T', f'{runtime}',
                   '-P', f'{report_sample_interval}']

extra_args_read = ['-c', '2', '-j', '2',
                   '--builtin=select-only',
                   '-l', f'--log-prefix={read_log_prefix}',
                   '-R', '1000000' ]

postgres = Postgres()
pgbench = Pgbench(postgres, resultsdir)

wal_collector = WALCollector('wal_out')
collectors = [wal_collector]
signaler = Signaler(collectors)
with signaler.signal("initialize"):
    postgres.initialize()
    pgbench.pgbench_load()
    postgres.set_gucs()
    postgres.create_extensions()
    pgbench.create_pgbench_indexes()

with signaler.signal("restart"):
    postgres.restart()
    pgbench.pgbench_prewarm()

postgres.reset_stats()

with signaler.signal("run"):
    pgbench.pgbench_run_and_log(extra_args_both + extra_args_read)

postgres.reset_stats()

with signaler.signal("vacuum"):
    pgbench.pgbench_vacuum()

with signaler.signal("cleanup"):
    postgres.cleanup()
