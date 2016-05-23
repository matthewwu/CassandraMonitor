from subprocess import PIPE, Popen, STDOUT
from statsd import StatsClient
import sys

keyspace = sys.argv[1]

GRAPHITE_URL = '10.1.1.5'
GRAPHITE_PORT = 8125
GRAPHITE_PREFIX = 'cmb_cs'

statsd_client = StatsClient(GRAPHITE_URL, GRAPHITE_PORT, prefix=GRAPHITE_PREFIX)

GRAFANA_MAP = {
    '99%': 'percentile_99',
    '95%': 'percentile_95',
    '50%': 'percentile_50',
}

COLUMN_INDEX = {
    'Write_Latency': 3,
    'Read_Latency': 4
}

KEYSPACE_MAP = {
    'weights': ['weights_from_profile', 'weights_to_profile', 'potentials_by_profile']
}


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


class Cfhistogram(object):

    def __init__(self, keyspace, tables):
        self.keyspace = keyspace

    def get_cfhistogram(self, keyspace, table):
        cmd = 'nodetool cfhistograms {} {}'.format(keyspace, table)
        print ('running command:{}'.format(cmd))
        retcode, output = run_command(
            cmd,
            raise_on_error=True)

        return self._get_result_dict(output, keyspace, table)

    def _get_result_dict(self, result, keyspace, table):
        table_stats = {'Table': table}
        lines = result.split('\n')
        for line in lines:
            items = line.split(' ')
            items = self._cleanup(items)
            for item in items:
                if item in GRAFANA_MAP.keys():
                    metric = GRAFANA_MAP[item]



    def _cleanup(self, items):
        new_items = []
        for item in items:
            if len(item) > 0:
                new_items.append(item)
        return new_items