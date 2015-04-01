#!/usr/bin/python
import json
import os

import pymongo
import ezodf

import perf_parse

con = pymongo.MongoClient()
db = con['apf']
coll = db['results']


MEM_SIZES = (256, 277, 298, 320, 341, 362, 384, 512, 1024, 2048)
MEM_SIZES = (256, 277, 298, 320, 384, 512, 1024, 2048)
MEM_SIZES = (256, 298, 341, 384, 512, 1024, 2048)
# MEM_SIZES = (256, )

ROWS = 20

key_mem_size = lambda x: x['machine_spec']['mem_size']
key_cgroup_limit = lambda x: x['cgroup_limit']
key_cgroup_limit_none_fix = lambda x: key_cgroup_limit(x) or 2048

pred_memcached = lambda x: x['test']['name'] == 'memcached_test_mini'
pred_apache = lambda x: x['test']['name'] == 'apache_test'
pred_node = lambda x: x['test']['name'] == 'node_test'
pred_psql = lambda x: x['test']['name'] == 'pgbench_test'

pred_never = lambda x: False


# memcached results
memcached_optimum = {m: [] for m in MEM_SIZES}
memcached_with_fix = {m: [] for m in MEM_SIZES}
memcached_without_fix = {m: [] for m in MEM_SIZES}

# apache results
apache_optimum = {m: [] for m in MEM_SIZES}
apache_with_fix = {m: [] for m in MEM_SIZES}
apache_without_fix = {m: [] for m in MEM_SIZES}

# node
node_optimum = {m: [] for m in MEM_SIZES}
node_with_fix = {m: [] for m in MEM_SIZES}
node_without_fix = {m: [] for m in MEM_SIZES}

# postgresql
postgresql_optimum = {m: [] for m in MEM_SIZES}
postgresql_with_fix = {m: [] for m in MEM_SIZES}
postgresql_without_fix = {m: [] for m in MEM_SIZES}

_PREFIXED = lambda img: os.path.join(
    '/home/dkuznets/projects/school/apf-images',
    img,
)
TEMPLATE_CLEAN = _PREFIXED('fedora20-clean.qcow2.template')
TEMPLATE_FIX = _PREFIXED('fedora20-withfix7.qcow2.template')
TEMPLATE_NOFIX = _PREFIXED('fedora20-withoutfix7.qcow2.template')

memcached_lookup = {
    TEMPLATE_CLEAN: (
        memcached_optimum,
        key_mem_size,
        pred_memcached,
    ),
    TEMPLATE_FIX: (
        memcached_with_fix,
        key_cgroup_limit,
        pred_memcached,
    ),
    TEMPLATE_NOFIX: (
        memcached_without_fix,
        key_cgroup_limit,
        pred_memcached,
    ),
}
apache_lookup = {
    TEMPLATE_CLEAN: (
        apache_optimum,
        key_mem_size,
        pred_apache,
    ),
    TEMPLATE_FIX: (
        apache_with_fix,
        key_cgroup_limit_none_fix,
        pred_apache,
    ),
    TEMPLATE_NOFIX: (
        apache_without_fix,
        key_cgroup_limit_none_fix,
        pred_apache,
    ),
}
node_lookup = {
    TEMPLATE_CLEAN: (
        node_optimum,
        key_mem_size,
        pred_node,
    ),
    TEMPLATE_FIX: (
        node_with_fix,
        key_cgroup_limit_none_fix,
        pred_node,
    ),
    TEMPLATE_NOFIX: (
        node_without_fix,
        key_cgroup_limit_none_fix,
        pred_node,
    ),
}
postgresql_lookup = {
    TEMPLATE_CLEAN: (
        postgresql_optimum,
        key_mem_size,
        pred_psql,
    ),
    TEMPLATE_FIX: (
        postgresql_with_fix,
        key_cgroup_limit_none_fix,
        pred_psql,
    ),
    TEMPLATE_NOFIX: (
        postgresql_without_fix,
        key_cgroup_limit_none_fix,
        pred_psql,
    ),
}

query = {
    '$or': [
        {
            'test.name': 'memcached_test_mini',
            'test.kwargs.count': 100 * 1000,
            'test.kwargs.key_limit': 40960,
        },
        {
            'test.name': 'apache_test',
            'test.kwargs.requests': 100 * 1000,
            'test.kwargs.concurrency': 75,
            'test.result.exitcode': 0,
        },
        {
            'test.name': 'node_test',
            'test.kwargs.requests': 100 * 1000,
            'test.kwargs.concurrency': 75,
            'test.result.exitcode': 0,
        },
        {
            'test.name': 'pgbench_test',
            'test.kwargs.scale': 100,
            'test.kwargs.clients': 10,
            'test.kwargs.transactions': 450,
            'test.result.exitcode': 0,
        },
    ]
}


def do_lookup():
    for rec in con['apf']['results'].find(query):
        lookup = {
            'memcached_test_mini': memcached_lookup,
            'apache_test': apache_lookup,
            'node_test': node_lookup,
            'pgbench_test': postgresql_lookup,
        }[rec['test']['name']]

        res_dict, key, pred = lookup.get(
            rec['machine_spec']['template_path'],
            ({}, None, pred_never)
        )
        if pred(rec) and key(rec) in MEM_SIZES:
            res_dict[key(rec)].append(rec)
do_lookup()


def print_res_stats(results):
    for k, v in sorted(results.items()):
        if len(v):
            print k, len(v)

print 'memcached_optimum'
print_res_stats(memcached_optimum)
print 'memcached_with_fix'
print_res_stats(memcached_with_fix)
print 'memcached_without_fix'
print_res_stats(memcached_without_fix)

print 'apache_optimum'
print_res_stats(apache_optimum)
print 'apache_with_fix'
print_res_stats(apache_with_fix)
print 'apache_without_fix'
print_res_stats(apache_without_fix)

print 'node_optimum'
print_res_stats(node_optimum)
print 'node_with_fix'
print_res_stats(node_with_fix)
print 'node_without_fix'
print_res_stats(node_without_fix)

print 'postgresql_optimum'
print_res_stats(postgresql_optimum)
print 'postgresql_with_fix'
print_res_stats(postgresql_with_fix)
print 'postgresql_without_fix'
print_res_stats(postgresql_without_fix)


def set_row(sheet, row, values):
    for (index, value) in enumerate(values):
        sheet[(row, index)] = ezodf.Cell(value)


LEGEND_MEMCACHED = [
    'ID',
    'Test name',
    'Memory size',
    'Resident memory limit',
    'Requests/thread',
    'Threads',
    'Test success',
    'Duration',
    'Host swap read IO',
    'Host swap write IO',
    'Guest swap read IO',
    'Guest swap write IO',
    'Halt events',
    'Halt events without IRQ mask',
    'Adjusted duration',
]
LEGEND_APACHE = LEGEND_MEMCACHED
LEGEND_POSTGRESQL = [
    'ID',
    'Test name',
    'Memory size',
    'Resident memory limit',
    'Scale',
    'Clients',
    'Transactions',
    'Test success',
    'Duration',
    'Host swap read IO',
    'Host swap write IO',
    'Guest swap read IO',
    'Guest swap write IO',
    'Halt events',
    'Halt events without IRQ mask',
]


def block_stat(cont):
    return [
        int(e) for e in cont.strip('\x00').strip().split()
    ]

HOST_SWAP_PATH = '/sys/block/dm-1/stat'
GUEST_ROOT_PATH = '/sys/block/dm-0/stat'
GUEST_SWAP_PATH = '/sys/block/dm-1/stat'
GUEST_RANDFILES_PATH = set(
    [
        '/sys/block/dm-2/stat',
        '/sys/block/dm-3/stat',
    ],
)


def fix_files(res):
    res['pre'] = {
        e['path']: e['contents']
        for e in res['pre']
    }
    res['post'] = {
        e['path']: e['contents']
        for e in res['post']
    }


def memcached_res_to_row(res):
    fix_files(res['files']['host'])

    host_swap_pre = block_stat(res['files']['host']['pre'][HOST_SWAP_PATH])
    host_swap_post = block_stat(res['files']['host']['post'][HOST_SWAP_PATH])
    host_swap_delta = [
        post - pre for (pre, post) in zip(host_swap_pre, host_swap_post)
    ]

    try:
        guest_swap_pre = block_stat(
            res['files']['guest']['pre'][GUEST_SWAP_PATH]
        )
        guest_swap_post = block_stat(
            res['files']['guest']['post'][GUEST_SWAP_PATH]
        )
        guest_swap_delta = [
            post - pre for (pre, post) in zip(guest_swap_pre, guest_swap_post)
        ]
    except KeyError:
        guest_swap_delta = [None] * 8

    events = 'N/A'
    events_noirq = 'N/A'
    if res['perf']:
        all_events = list(perf_parse.parse_perf_output(res['perf']['output']))
        events = len(all_events)
        events_noirq = len(
            [
                e for e in all_events
                if int(e[1].split('=')[-1].strip()) < 256
            ]
        )

    return [
        str(res['_id']),
        res['test']['name'],
        res['machine_spec']['mem_size'],
        res['cgroup_limit'] or 'None',
        res['test']['kwargs']['count'],
        res['test']['kwargs']['concurrency'],
        1,
        res['test']['result'],
        host_swap_delta[0],
        host_swap_delta[4],
        guest_swap_delta[0],
        guest_swap_delta[4],
        events,
        events_noirq,
    ]


def memcached_results_to_sheet(name, results):
    print name
    sheet = ezodf.Sheet(name, size=(1000, 100))
    set_row(sheet, 0, LEGEND_MEMCACHED)
    for row, res in enumerate(map(memcached_res_to_row, results[-ROWS:])):
        set_row(sheet, row + 1, res)
    return sheet


def apache_res_to_row(res):
    fix_files(res['files']['host'])
    if type(res['files']['guest']['pre']) == list:
        fix_files(res['files']['guest'])

    host_swap_pre = block_stat(res['files']['host']['pre'][HOST_SWAP_PATH])
    host_swap_post = block_stat(res['files']['host']['post'][HOST_SWAP_PATH])
    host_swap_delta = [
        post - pre for (pre, post) in zip(host_swap_pre, host_swap_post)
    ]

    try:
        guest_swap_pre = block_stat(
            res['files']['guest']['pre'][GUEST_SWAP_PATH]
        )
        guest_swap_post = block_stat(
            res['files']['guest']['post'][GUEST_SWAP_PATH]
        )
        guest_swap_delta = [
            post - pre for (pre, post) in zip(guest_swap_pre, guest_swap_post)
        ]
    except KeyError:
        guest_swap_delta = [None] * 8

    try:
        exit_code = res['test']['result']['exitcode'] == 0 and 1 or 0
    except Exception:
        exit_code = 0

    try:
        duration = [
            float(line.split()[-2])
            for line in res['test']['result']['stdout'].split('\n')
            if line.startswith('Time taken for tests')
        ][0]
    except Exception:
        duration = None
    events = 'N/A'
    events_noirq = 'N/A'
    if res['perf']:
        all_events = list(perf_parse.parse_perf_output(res['perf']['output']))
        events = len(all_events)
        events_noirq = len(
            [
                e for e in all_events
                if int(e[1].split('=')[-1].strip()) < 256
            ]
        )
    return [
        str(res['_id']),
        res['test']['name'],
        res['machine_spec']['mem_size'],
        res['cgroup_limit'] or 'None',
        res['test']['kwargs']['requests'],
        res['test']['kwargs']['concurrency'],
        exit_code,
        duration,
        host_swap_delta[0],
        host_swap_delta[4],
        guest_swap_delta[0],
        guest_swap_delta[4],
        events,
        events_noirq,
        (
            int(host_swap_delta[0])
            and str(float(duration) / float(host_swap_delta[0]))
        ),
    ]


def postgresql_res_to_row(res):
    fix_files(res['files']['host'])
    if type(res['files']['guest']['pre']) == list:
        fix_files(res['files']['guest'])

    host_swap_pre = block_stat(res['files']['host']['pre'][HOST_SWAP_PATH])
    host_swap_post = block_stat(res['files']['host']['post'][HOST_SWAP_PATH])
    host_swap_delta = [
        post - pre for (pre, post) in zip(host_swap_pre, host_swap_post)
    ]

    try:
        guest_swap_pre = block_stat(
            res['files']['guest']['pre'][GUEST_SWAP_PATH]
        )
        guest_swap_post = block_stat(
            res['files']['guest']['post'][GUEST_SWAP_PATH]
        )
        guest_swap_delta = [
            post - pre for (pre, post) in zip(guest_swap_pre, guest_swap_post)
        ]
    except KeyError:
        guest_swap_delta = [None] * 8

    try:
        exit_code = res['test']['result']['exitcode'] == 0 and 1 or 0
    except Exception:
        exit_code = 0

    events = 'N/A'
    events_noirq = 'N/A'
    if res['perf']:
        all_events = list(perf_parse.parse_perf_output(res['perf']['output']))
        events = len(all_events)
        events_noirq = len(
            [
                e for e in all_events
                if int(e[1].split('=')[-1].strip()) < 256
            ]
        )

    return [
        str(res['_id']),
        res['test']['name'],
        res['machine_spec']['mem_size'],
        res['cgroup_limit'] or 'None',
        res['test']['kwargs']['scale'],
        res['test']['kwargs']['clients'],
        res['test']['kwargs']['transactions'],
        exit_code,
        res['test']['result']['duration'],
        host_swap_delta[0],
        host_swap_delta[4],
        guest_swap_delta[0],
        guest_swap_delta[4],
        events,
        events_noirq,
    ]


def apache_test_parser(res):
    try:
        exit_code = res['test']['result']['exitcode'] == 0 and 1 or 0
    except Exception:
        exit_code = 0

    try:
        duration = [
            float(line.split()[-2])
            for line in res['test']['result']['stdout'].split('\n')
            if line.startswith('Time taken for tests')
        ][0]
    except Exception:
        duration = None
    return {
        'name': res['test']['name'],
        'params': {
            'requests': res['test']['kwargs']['requests'],
            'concurrency': res['test']['kwargs']['concurrency'],
        },
        'results': {
            'success': exit_code,
            'duration': duration,
        },
    }


def memcached_parser(res):
    return {
        'name': res['test']['name'],
        'params': {
            'count': res['test']['kwargs']['count'],
            'concurrency': res['test']['kwargs']['concurrency'],
        },
        'results': {
            'success': 1,
            'duration': res['test']['result'],
        },
    }


def pgbench_parser(res):
    try:
        exit_code = res['test']['result']['exitcode'] == 0 and 1 or 0
    except Exception:
        exit_code = 0
    return {
        'name': res['test']['name'],
        'params': {
            'scale': res['test']['kwargs']['scale'],
            'clients': res['test']['kwargs']['clients'],
            'transactions': res['test']['kwargs']['transactions'],
        },
        'results': {
            'success': exit_code,
            'duration': res['test']['result']['duration'],
        },
    }

TEST_PARSERS = {
    'apache_test': apache_test_parser,
    'node_test': apache_test_parser,
    'memcached_test_mini': memcached_parser,
    'pgbench_test': pgbench_parser,
}


def transform_result(res):
    fix_files(res['files']['host'])
    if type(res['files']['guest']['pre']) == list:
        fix_files(res['files']['guest'])

    host_swap_pre = block_stat(res['files']['host']['pre'][HOST_SWAP_PATH])
    host_swap_post = block_stat(res['files']['host']['post'][HOST_SWAP_PATH])
    host_swap_delta = [
        post - pre for (pre, post) in zip(host_swap_pre, host_swap_post)
    ]

    try:
        guest_swap_pre = block_stat(
            res['files']['guest']['pre'][GUEST_SWAP_PATH]
        )
        guest_swap_post = block_stat(
            res['files']['guest']['post'][GUEST_SWAP_PATH]
        )
        guest_swap_delta = [
            post - pre for (pre, post) in zip(guest_swap_pre, guest_swap_post)
        ]
    except KeyError:
        guest_swap_delta = [None] * 8

    try:
        guest_root_pre = block_stat(
            res['files']['guest']['pre'][GUEST_ROOT_PATH]
        )
        guest_root_post = block_stat(
            res['files']['guest']['post'][GUEST_ROOT_PATH]
        )
        guest_root_delta = [
            post - pre for (pre, post) in zip(guest_root_pre, guest_root_post)
        ]
    except KeyError:
        guest_root_delta = [None] * 8

    try:
        RF = list(
            set(
                res['files']['guest']['pre'].keys()
            ).intersection(GUEST_RANDFILES_PATH)
        ).pop()
        guest_rf_pre = block_stat(res['files']['guest']['pre'][RF])
        guest_rf_post = block_stat(res['files']['guest']['post'][RF])
        guest_rf_delta = [
            post - pre for (pre, post) in zip(guest_rf_pre, guest_rf_post)
        ]
    except KeyError:
        guest_rf_delta = [None] * 8
    events = None
    events_noirq = None
    if res['perf']:
        all_events = list(perf_parse.parse_perf_output(res['perf']['output']))
        events = len(all_events)
        events_noirq = len(
            [
                e for e in all_events
                if int(e[1].split('=')[-1].strip()) < 256
            ]
        )
    return {
        'id': str(res['_id']),
        'type': res['type'],
        'memory': {
            'total': res['machine_spec']['mem_size'],
            'cgroup_limit': res['cgroup_limit'] or None,
        },
        'disk_activity': {
            'host': {
                'swap': {
                    'read': host_swap_delta[0],
                    'write': host_swap_delta[4],
                },
            },
            'guest': {
                'swap': {
                    'read':  guest_swap_delta[0],
                    'write': guest_swap_delta[4],
                },
                'rootfs': {
                    'read': guest_root_delta[0],
                    'write': guest_root_delta[4],
                },
                'rf': {
                    'read': guest_rf_delta[0],
                    'write': guest_rf_delta[4],
                },
            },
        },
        'test': TEST_PARSERS[res['test']['name']](res),
        'events': {
            'total': events,
            'noirq': events_noirq,
        },
    }


def apache_results_to_sheet(name, results):
    print name
    sheet = ezodf.Sheet(name, size=(1000, 100))
    set_row(sheet, 0, LEGEND_MEMCACHED)
    for row, res in enumerate(map(apache_res_to_row, results[-ROWS:])):
        set_row(sheet, row + 1, res)
    return sheet


def postgresql_results_to_sheet(name, results):
    print name
    sheet = ezodf.Sheet(name, size=(1000, 100))
    set_row(sheet, 0, LEGEND_POSTGRESQL)
    for row, res in enumerate(map(postgresql_res_to_row, results[-ROWS:])):
        set_row(sheet, row + 1, res)
    return sheet


def sheet_col_average(sheet, col_name):
    legend = [sheet[(0, i)].value for i in range(30)]
    legend = [l for l in legend if l]
    col = legend.index(col_name)
    succ_col = legend.index('Test success')
    cells = [
        sheet[(i, col)].value
        for i in range(1, sheet.nrows())
        if sheet[(i, succ_col)].value == 1
    ]
    return sum(cells) / len(cells)


def fill_summary(ods, summary):
    layout = [
        ['Apache'],
        [
            'Memory pressure',
            'Optimum average time',
            'Fixed average time',
            'Fixed average halt events',
            'Unfixed averag time',
            'Unfixed average halt events',
        ]
    ]

    for mem_size in MEM_SIZES:
        layout.append(
            [
                mem_size,
                sheet_col_average(
                    ods.sheets['apache_optimum-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['apache_with_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['apache_with_fix-%d' % mem_size],
                    'Halt events'
                ),
                sheet_col_average(
                    ods.sheets['apache_without_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['apache_without_fix-%d' % mem_size],
                    'Halt events'
                ),
            ]
        )
    layout.extend([
        ['memcached'],
        [
            'Memory pressure',
            'Optimum average time',
            'Fixed average time',
            'Fixed average halt events',
            'Unfixed averag time',
            'Unfixed average halt events',
        ],
    ])
    for mem_size in MEM_SIZES:
        layout.append(
            [
                mem_size,
                sheet_col_average(
                    ods.sheets['memcached_optimum-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['memcached_with_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['memcached_with_fix-%d' % mem_size],
                    'Halt events'
                ),
                sheet_col_average(
                    ods.sheets['memcached_without_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['memcached_without_fix-%d' % mem_size],
                    'Halt events'
                ),
            ]
        )

    layout.extend([
        ['node'],
        [
            'Memory pressure',
            'Optimum average time',
            'Fixed average time',
            'Fixed average halt events',
            'Unfixed averag time',
            'Unfixed average halt events',
        ],
    ])
    for mem_size in MEM_SIZES:
        layout.append(
            [
                mem_size,
                sheet_col_average(
                    ods.sheets['node_optimum-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['node_with_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['node_with_fix-%d' % mem_size],
                    'Halt events'
                ),
                sheet_col_average(
                    ods.sheets['node_without_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['node_without_fix-%d' % mem_size],
                    'Halt events'
                ),
            ]
        )

    layout.extend([
        ['postgresql'],
        [
            'Memory pressure',
            'Optimum average time',
            'Fixed average time',
            'Fixed average halt events',
            'Unfixed averag time',
            'Unfixed average halt events',
        ],
    ])
    for mem_size in MEM_SIZES:
        layout.append(
            [
                mem_size,
                sheet_col_average(
                    ods.sheets['postgresql_optimum-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['postgresql_with_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['postgresql_with_fix-%d' % mem_size],
                    'Halt events'
                ),
                sheet_col_average(
                    ods.sheets['postgresql_without_fix-%d' % mem_size],
                    'Duration'
                ),
                sheet_col_average(
                    ods.sheets['postgresql_without_fix-%d' % mem_size],
                    'Halt events'
                ),
            ]
        )

    for i, row in enumerate(layout):
        set_row(summary, i, row)


def export_ods(path):
    ods = ezodf.newdoc(doctype='ods', filename=path)
    summary = ezodf.Sheet('summary', size=(100, 100))
    ods.sheets += summary
    for mem_size, results in sorted(memcached_optimum.items()):
        if not len(results):
            continue
        ods.sheets += memcached_results_to_sheet(
            'memcached_optimum-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(memcached_with_fix.items()):
        if not len(results):
            continue
        if mem_size is None:
            mem_size = 'unrestrained'
        ods.sheets += memcached_results_to_sheet(
            'memcached_with_fix-%s' % str(mem_size),
            results
        )
    for mem_size, results in sorted(memcached_without_fix.items()):
        if not len(results):
            continue
        if mem_size is None:
            mem_size = 'unrestrained'
        ods.sheets += memcached_results_to_sheet(
            'memcached_without_fix-%s' % str(mem_size),
            results
        )

    for mem_size, results in sorted(apache_optimum.items()):
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'apache_optimum-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(apache_with_fix.items()):
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'apache_with_fix-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(apache_without_fix.items()):
        if mem_size is None:
            mem_size = 2048
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'apache_without_fix-%d' % mem_size,
            results
        )

    for mem_size, results in sorted(node_optimum.items()):
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'node_optimum-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(node_with_fix.items()):
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'node_with_fix-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(node_without_fix.items()):
        if mem_size is None:
            mem_size = 2048
        if not len(results):
            continue
        ods.sheets += apache_results_to_sheet(
            'node_without_fix-%d' % mem_size,
            results
        )

    for mem_size, results in sorted(postgresql_optimum.items()):
        if not len(results):
            continue
        ods.sheets += postgresql_results_to_sheet(
            'postgresql_optimum-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(postgresql_with_fix.items()):
        if not len(results):
            continue
        ods.sheets += postgresql_results_to_sheet(
            'postgresql_with_fix-%d' % mem_size,
            results
        )
    for mem_size, results in sorted(postgresql_without_fix.items()):
        if not len(results):
            continue
        ods.sheets += postgresql_results_to_sheet(
            'postgresql_without_fix-%d' % mem_size,
            results
        )

    fill_summary(ods, summary)
    ods.save()
# export_ods('results.ods')


def simplify_event(event):
    return (event[0], event[1], tuple((frame[0],) for frame in event[2]))


def export_events():
    events_histogram = {}
    for name, collection in (
        ('apache_with_fix', apache_with_fix),
        ('apache_without_fix', apache_without_fix),
        ('memcached_with_fix', memcached_with_fix),
        ('memcached_without_fix', memcached_without_fix),
        ('node_with_fix', node_with_fix),
        ('node_without_fix', node_without_fix),
        ('postgresql_with_fix', postgresql_with_fix),
        ('postgresql_without_fix', postgresql_without_fix),
    ):
        for mem_pressure, tests in collection.items():
            for test in tests[-10:]:
                for event in perf_parse.parse_perf_output(
                    test['perf']['output']
                ):
                    event = simplify_event(event)
                    if event not in events_histogram:
                        events_histogram[event] = {
                            'by_test': {},
                            'by_mem_pressure': {}
                        }
                    events_histogram[event]['by_test'][name] = (
                        events_histogram[event]['by_test'].get(name, 0) + 1
                    )
                    events_histogram[event]['by_mem_pressure'][
                        mem_pressure
                    ] = (
                        events_histogram[event]['by_mem_pressure'].get(
                            mem_pressure, 0
                        ) + 1
                    )
    return events_histogram


def export_events_ods(path):
    hist = export_events()
    COLUMNS = [
        'Event',
        'Preempt count',
        'Stack trace',
        'Total',
        'with_fix',
        'without_fix',
        'apache_with_fix',
        'apache_without_fix',
        'memcached_with_fix',
        'memcached_without_fix',
        'node_with_fix',
        'node_without_fix',
        'postgresql_with_fix',
        'postgresql_without_fix',
    ] + [
        '%d MB' % mem_pressure
        for mem_pressure in MEM_SIZES
    ]
    rows = []
    for event, stats in hist.items():
        rows.append([
            event[0],
            int(event[1].split('=')[-1]),
            ','.join([frame[0] for frame in event[2]]),
            sum(stats['by_test'].values()),
            (
                stats['by_test'].get('apache_with_fix', 0) +
                stats['by_test'].get('memcached_with_fix', 0) +
                stats['by_test'].get('node_with_fix', 0) +
                stats['by_test'].get('postgresql_with_fix', 0)
            ),
            (
                stats['by_test'].get('apache_without_fix', 0) +
                stats['by_test'].get('memcached_without_fix', 0) +
                stats['by_test'].get('node_without_fix', 0) +
                stats['by_test'].get('postgresql_without_fix', 0)
            ),
            stats['by_test'].get('apache_with_fix', 0),
            stats['by_test'].get('apache_without_fix', 0),
            stats['by_test'].get('memcached_with_fix', 0),
            stats['by_test'].get('memcached_without_fix', 0),
            stats['by_test'].get('node_with_fix', 0),
            stats['by_test'].get('node_without_fix', 0),
            stats['by_test'].get('postgresql_with_fix', 0),
            stats['by_test'].get('postgresql_without_fix', 0),
        ] + [
            stats['by_mem_pressure'].get(mem_pressure, 0)
            for mem_pressure in MEM_SIZES
        ])
    sorted_rows = sorted(rows, key=lambda x: int(x[3]), reverse=True)
    sheet = ezodf.Sheet('events', size=(4000, 100))
    set_row(sheet, 0, COLUMNS)
    for i, row in enumerate(sorted_rows):
        set_row(sheet, i + 1, row)
    ods = ezodf.newdoc(doctype='ods', filename=path)
    ods.sheets += sheet
    ods.save()
# export_events_ods('events.ods')


def add_tags():
    for tag, cols in {
        'with_fix': [
            apache_with_fix,
            node_with_fix,
            memcached_with_fix,
            postgresql_with_fix,
        ],
        'without_fix': [
            apache_without_fix,
            node_without_fix,
            memcached_without_fix,
            postgresql_without_fix,
        ],
        'optimum': [
            apache_optimum,
            node_optimum,
            memcached_optimum,
            postgresql_optimum,
        ],
    }.items():
        for col in cols:
            for results in col.values():
                for res in results:
                    res['type'] = tag
add_tags()


def export_json(path):
    lst = []
    for col in [
        apache_with_fix,
        apache_without_fix,
        apache_optimum,
        node_with_fix,
        node_without_fix,
        node_optimum,
        postgresql_with_fix,
        postgresql_without_fix,
        postgresql_optimum,
        memcached_with_fix,
        memcached_without_fix,
        memcached_optimum,
    ]:
        for results in col.values():
            results = results[-ROWS:]
            lst.extend(map(transform_result, results))
    with open(path, 'w') as f:
        json.dump(lst, f, indent=4)
export_json('test.json')
