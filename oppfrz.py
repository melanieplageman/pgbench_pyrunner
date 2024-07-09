import os
import tempfile
import datetime

from pgbench_runner import *

benchroot="/tmp/frzstats/switchinsertupdate/"

run_specific_gucs = {
                    'opp_freeze_algo':1,
                    'target_freeze_duration':1000,
                     }

other_run_variables = {
    'fillfactor' : 50,
    }

runvars = {**other_run_variables, **run_specific_gucs}

resultsdir = benchroot + "_".join([f"{k}-{v}" for k, v in runvars.items()])

os.makedirs(resultsdir, exist_ok=True)
os.chdir(resultsdir)

runtime = 10
report_sample_interval = 1

postgres = Postgres()
pgbench = PgbenchSwitchInsertUpdate()

wal_collector = WALCollector('pgstatwal.raw')
pgstatio_collector = PgStatIOCollector('pgstatio.raw')
relfrozenxid_collector = RelfrozenxidCollector('relfrozenxid.raw')
alltables_collector = PgStatAllTablesCollector('insert_update_shift',
                                               'alltables.raw')
collectors = [
        wal_collector,
        pgstatio_collector,
        relfrozenxid_collector,
        alltables_collector,
        ]

signaler = Signaler(collectors)

with signaler.signal("initialize"):
    postgres.initialize()
    pgbench.load(other_run_variables['fillfactor'])
    postgres.set_gucs(**run_specific_gucs)
    postgres.create_extensions()
    pgbench.create_indexes()

with signaler.signal("restart"):
    postgres.restart()
    pgbench.prewarm()

postgres.force_flush_stats()
postgres.reset_stats()

os.makedirs('execution_reports', exist_ok=True)

with signaler.signal("run"):
    common_args = ['-T', str(runtime), '-P', str(report_sample_interval)]

    write_args = common_args + [
        '-c', '16',
        '-j', '16',
    ]
    execution_reports_arts = [
        '-l',
        '--log-prefix=execution_reports/su',
    ]

    pgbench.run_and_log('insert', *write_args)
    postgres.force_flush_stats()
    pgbench.run_and_log('update', *write_args)
    postgres.force_flush_stats()

with signaler.signal("vacuum"):
    pgbench.vacuum()
    postgres.force_flush_stats()

with signaler.signal("cleanup"):
    pass
