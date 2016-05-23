from subprocess import PIPE, Popen, STDOUT
from statsd import StatsClient
import sys

keyspace = sys.argv[1]

GRAPHITE_URL = '10.1.1.5'
GRAPHITE_PORT = 8125
GRAPHITE_PREFIX = 'cmb_cs'

statsd_client = StatsClient(GRAPHITE_URL, GRAPHITE_PORT, prefix=GRAPHITE_PREFIX)

CF_STATS_MAP = {
# "Keyspace": "Keyspace",
"Table": "Column Family",
"SSTable count": 1,
"Space used (live)": 2,
"Space used (total)": 3,
"Number of Keys (estimate)": 4,
"Memtable Columns Count": 5,
"Memtable Data Size": 6,
"Memtable Switch Count": 7,
"Local read count": 8,
"Local read latency": 9,
"Local write count": 10,
"Local write latency": 11,
"Pending flushes": 12,
"Bloom Filter False Postives": 13,
"Bloom Filter False Ratio": 14,
"Bloom Filter Space Used": 15,
"Compacted row minimum size": 16,
"Compacted row maximum size": 17,
"Compacted row mean size": 18
}


GRAFANA_MAP = {
    "Local read latency": "read_latency",
    "Local write latency": "write_latency"
}


def get_grafana_metric_name(keyspace, table, metric):
    return 'cass_{}_{}_{}'.format(keyspace, metric, table)


def run_command(cmd_to_run,get_output=True,raise_on_error=False):
    p = Popen(cmd_to_run, stdout=PIPE, stderr=STDOUT, shell=True)
    output = []
    line = p.stdout.readline()
    while line:
        line = line[:-1]
        if get_output:
            output.append(line)

        line = p.stdout.readline()

    p.wait()
    if raise_on_error and p.returncode != 0:
        raise Exception("Command execution failed")
    if get_output:
        return (p.returncode,"\n".join(output))
    else:
        return p.returncode


class CfStat(object):

    def __init__(self, keyspace):
        self.keyspace = keyspace

    def get_cfstats(self):
        cmd = 'nodetool cfstats {}'.format(self.keyspace)
        print ('running command:{}'.format(cmd))
        retcode, output = run_command(
            cmd,
            raise_on_error=True)

        return self._get_result_dict(output, self.keyspace)

    def _get_result_dict(self, cf_result, keyspace):
        keyspace_stats = {}
        table_results = cf_result.split('\n\n\t\t')
        for table_result in table_results:
            try:
                table_stats = dict()
                table_stats['Keyspace'] = keyspace
                results = table_result.split('\n\t')
                for line in results:
                    if ':' in line:
                        item = line.lstrip()
                        metric = item.split(':')
                        name = metric[0].lstrip()
                        if name in CF_STATS_MAP.keys():
                            table_stats[name] = metric[1].lstrip()
                keyspace_stats[table_stats['Table']] = table_stats
            except:
                pass
        return keyspace_stats


class MetricRunner(object):

    def __init__(self, keyspace):
        self.keyspace = keyspace

    def run(self):
        cf = CfStat(self.keyspace)
        keyspace_stat = cf.get_cfstats()
        for table, stat in keyspace_stat.iteritems():
            for cf_key, ga_key in GRAFANA_MAP.iteritems():
                metric = stat[cf_key]
                grafana_metric_name = get_grafana_metric_name(self.keyspace, table, ga_key)
                if 'ms' in metric:
                    time = metric.split(' ')[0]
                    time = time.lstrip()
                    self._send(grafana_metric_name, float(time))

    def _send(self, name, time_ms):
        statsd_client.timing(name, time_ms)


runner = MetricRunner(keyspace)
runner.run()

