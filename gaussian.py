import os
import psycopg
from pgbench_runner import Signaler
from pgbench_runner import ScalarCollector, SeriesCollector, IntervalCollector
from pgbench_runner import WALCollector, PgStatIOCollector
from pgbench_runner import Postgres, Pgbench

resultsdir = '/tmp/pgresults'

runtime = 10
report_sample_interval = 2
read_log_prefix = os.path.join(resultsdir, 'execution_reports',
                               'pgbench_log_read')
write_log_prefix = os.path.join(resultsdir, 'execution_reports',
                                'pgbench_log_write')

extra_args_both = ['-T', f'{runtime}',
                   '-P', f'{report_sample_interval}']

extra_args_read = ['-c', '2', '-j', '2',
                   '--builtin=select-only',
                   '-l', f'--log-prefix={read_log_prefix}',
                   '-R', '1000000' ]

extra_args_write = ['-c', '16', '-j', '16',
                   '-l', f'--log-prefix={write_log_prefix}', ]

postgres = Postgres()
pgbench = Pgbench(postgres, resultsdir)

wal_collector = WALCollector(os.path.join(resultsdir, 'pgstatwal.raw'))
pgstatio_collector = PgStatIOCollector(os.path.join(resultsdir, 'pgstatio.raw'))
collectors = [wal_collector, pgstatio_collector]
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
    # pgbench.pgbench_run_and_log('read', extra_args_both + extra_args_read)
    pgbench.pgbench_run_and_log('write', extra_args_both + extra_args_write)

postgres.reset_stats()

with signaler.signal("vacuum"):
    pgbench.pgbench_vacuum()

with signaler.signal("cleanup"):
    postgres.cleanup()
