import standard
import os

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

standard.initdb()
standard.pgbench_load(resultsdir)
standard.set_gucs()
standard.create_extensions()
standard.create_pgbench_indexes()
standard.restart()
standard.pgbench_prewarm()
standard.reset_stats()
standard.pgbench_run_and_log(resultsdir, extra_args_both + extra_args_read)
standard.reset_stats()
standard.pgbench_vacuum()
