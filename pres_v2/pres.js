// Create tabs
google.load("visualization", "1.1", {packages:["corechart", "bar"]});
google.setOnLoadCallback(start);

Array.prototype.unique = function()
{
    var tmp = {}, out = [];
    for(var i = 0, n = this.length; i < n; ++i) {
        if(!tmp[this[i]]) { tmp[this[i]] = true; out.push(this[i]); }
    }
    return out;
}

function simple_col (title) {
    return {'title': title};
}

var summary_columns = [
    simple_col('Memory size'),
    simple_col('Average optimum running time'),
    simple_col('Average optimum halt events'),
    simple_col('Average fixed running time'),
    simple_col('Average fixed halt events'),
    simple_col('Average not fixed running time'),
    simple_col('Average not fixed halt events'),
];

function all_test(test, results) {
    return results.filter(function (x) {
        return x.test.name == test;
    });
}

var rand_files_snippet = {
    title: 'Rand-files IO / Time',
    legend: [
        'Time taken',
        'Reads, optimum',
        'Writes, optimum',
        'Reads, with fix',
        'Writes, with fix',
        'Reads, without fix',
        'Writes, without fix',
    ],
    row_func: function (res) {
        var read = res.disk_activity.guest.rf.read;
        var write = res.disk_activity.guest.rf.write;
        return [
            res.test.results.duration,
            res.type == 'optimum' ? read : null,
            res.type == 'optimum' ? write : null,
            res.type == 'with_fix' ? read : null,
            res.type == 'with_fix' ? write : null,
            res.type == 'without_fix' ? read : null,
            res.type == 'without_fix' ? write : null,
        ];
    },
};

var host_swap_snippet = {
    title: 'Host swap IO / Time',
    legend: [
        'Time taken',
        'Reads, with fix',
        'Writes, with fix',
        'Reads, without fix',
        'Writes, without fix',
    ],
    row_func: function (res) {
        var read = res.disk_activity.host.swap.read;
        var write = res.disk_activity.host.swap.write;
        return [
            res.test.results.duration,
            res.type == 'with_fix' ? read : null,
            res.type == 'with_fix' ? write : null,
            res.type == 'without_fix' ? read : null,
            res.type == 'without_fix' ? write : null,
        ];
    },
};

var events_snippet = {
    title: 'Events / Time',
    legend: [
        'Duration',
        'Event, with fix',
        'Event (noIRQ), with fix',
        'Event, without fix',
        'Event (noIRQ), without fix',
    ],
    row_func: function (res) {
        var events = res.events.total;
        var noirq = res.events.noirq;
        return [
            res.test.results.duration,
            res.type == 'with_fix' ? events : null,
            res.type == 'with_fix' ? noirq : null,
            res.type == 'without_fix' ? events : null,
            res.type == 'without_fix' ? noirq : null,
        ];
    },
}


// Load DB data
var db = null;
function start() {
    $.getJSON(
        'db.json',
        function (data) {
            db = data;
            build_tab(
                $("#tabs-apache"), 
                all_test('apache_test', db),
                [
                    rand_files_snippet,
                    host_swap_snippet,
                    events_snippet,
                ]
            );
            build_tab(
                $("#tabs-node"),
                all_test('node_test', db),
                [
                    rand_files_snippet,
                    host_swap_snippet,
                    events_snippet,
                ]
            );
            build_tab(
                $("#tabs-memcached"),
                all_test('memcached_test_mini', db),
                [
                    host_swap_snippet,
                    events_snippet,
                ]
            );
            build_tab(
                $("#tabs-postgresql"),
                all_test('pgbench_test', db),
                [
                    host_swap_snippet,
                    events_snippet,
                ]
            );
            window.setTimeout(function () { $("#tabs").tabs(); }, 1000);
        }
    );
};

function res_to_row (res) {
    return [
        res.type,
        res.memory.total,
        eff_mem(res),
        res.test.results.success,
        res.test.results.duration,
        res.disk_activity.guest.swap.read,
        res.disk_activity.guest.swap.write,
        res.disk_activity.guest.rootfs.read,
        res.disk_activity.guest.rootfs.write,
        res.disk_activity.guest.rf.read,
        res.disk_activity.guest.rf.write,
        res.disk_activity.host.swap.read,
        res.disk_activity.host.swap.write,
        res.events.total | 0,
        res.events.noirq | 0,

        res.id,
    ];
}

function sum (a, b) {
    return a + b;
}

function average(results) {
    if (results.length == 0) {
        console.trace();
    }
    return Number((results.reduce(sum) / results.length).toFixed(4));
}

function average_duration(results) {
    return average(results.map(function (x) { return x.test.results.duration; }));
}

function average_events(results) {
    return average(results.map(function (x) { return x.events.total; }));
}

function all_type(t, results) {
    return results.filter(function (x) { return x.type == t; }); 
}

function all_opt (results) {
    return all_type('optimum', results);
}

function all_with (results) {
    return all_type('with_fix', results);
}

function all_without (results) {
    return all_type('without_fix', results);
}

function all_mem (mem, results) {
    return results.filter(function (x) {
        return eff_mem(x) == mem;
    });
}


function summary_row(results) {
    var opt = all_opt(results);
    var with_ = all_with(results);
    var without = all_without(results);

    return [
        eff_mem(results[0]),
        average_duration(opt),
        0,
        average_duration(with_),
        average_events(with_),
        average_duration(without),
        average_events(without),
    ];
}

function eff_mem(res) {
    return res.type == 'optimum' ? res.memory.total : res.memory.cgroup_limit;
}

function build_tab(root, results, per_mem_elements) {
    var mem_sizes = results.map(
        eff_mem
    ).unique().sort(function (a, b) { return a - b; });

    var summary_table = $(document.createElement('table'));
    root.append(
        $(document.createElement('table')).prop('id', 'summary')
    );

    root.find("#summary").dataTable({
        "data": mem_sizes.map(function (mem) {
            return summary_row(
                results.filter(
                    function (x) {
                        return eff_mem(x) == mem;
                    }
                )
            );
        }),
        "columns": summary_columns,
        "paging": false,
        "ordering": false,
        "info": false,
        "searching": false,
    });

    root.append(
        $(document.createElement('div'))
            .prop('id', 'graphs')
            .append(
                $(document.createElement('span'))
                    .prop('id', 'g-times')
                    .css('display', 'inline-block')
            )
            .append(
                $(document.createElement('span'))
                    .prop('id', 'g-events')
                    .css('display', 'inline-block')
            )
            .append(
                $(document.createElement('span'))
                    .prop('id', 'per-mem')
            )
    )
    
    root.append(
        $(document.createElement('div'))
            .prop('id', 'all-results-div')
            .css('clear', 'both')
            .append(
                $(document.createElement('h3')).html('All results')
            )
            .append(
                $(document.createElement('table')).prop('id', 'all-results')
            )
    )



    root.find("#all-results").dataTable({
        "data": results.map(res_to_row),
        "columns": [
            simple_col('Type'),
            simple_col('Total memory'),
            simple_col('Alloc. memory'),
            simple_col('Success'),
            simple_col('Duration'),
            simple_col('Guest swap reads'),
            simple_col('Guest swap writes'),
            simple_col('Guest rootfs reads'),
            simple_col('Guest rootfs writes'),
            simple_col('Guest rand-files reads'),
            simple_col('Guest rand-files writes'),
            simple_col('Host swap reads'),
            simple_col('Host swap writes'),
            simple_col('Halt events'),
            simple_col('Halt events (outside IRQ)'),

            simple_col('ID'),
        ],
        "paging": false,
        "ordering": true,
        "info": false,
        "searching": false,
    });
    root.find("#all-results-div").accordion({
        collapsible: true,
        active: false,
    });

    // Summary - times
    var options = {
        title: 'Average test running time',
        width: 700,
        height: 600,
        legend: {position: 'in'},
        vAxis: { title: 'Time in seconds' },
        bar: {groupWidth: "95%"},
    };

    var data = google.visualization.arrayToDataTable([
        ['Memory size', 'Optimum', 'With fix', 'Without fix'],
    ].concat(mem_sizes.map(function (mem) {
        ress = all_mem(mem, results);
        return [
            mem.toString() + ' MB',
            average_duration(all_opt(ress)),
            average_duration(all_with(ress)),
            average_duration(all_without(ress)),
        ]
    })));
    new google.visualization.ColumnChart(root.find("#g-times").get(0)).draw(data, options);

    // Summary - events
    options = {
        title: 'Average halt events',
        width: 700,
        height: 600,
        legend: {position: 'in'},
        vAxis: { title: 'Number of events' },
        bar: {groupWidth: "95%"},
    };

    var data = google.visualization.arrayToDataTable([
        ['Memory size', 'With fix', 'Without fix'],
    ].concat(mem_sizes.map(function (mem) {
        ress= all_mem(mem, results);
        return [
            mem.toString() + ' MB',
            average_events(all_with(ress)),
            average_events(all_without(ress)),
        ]
    })));
    new google.visualization.ColumnChart(root.find("#g-events").get(0)).draw(data, options);

    var per_mem_root = root.find("#per-mem");
    per_mem_elements.forEach(function (elem) {
        var title_string = elem.title;
        var legend = elem.legend;
        var row_func = elem.row_func;

        var per_mem = document.createElement('div');
        $(per_mem_root).append(per_mem);

        var title = document.createElement('h3');
        $(title).html(title_string);
        $(title).css('clear', 'both');
        $(per_mem).append(title);

        var per_mem_graphs = document.createElement('div');
        $(per_mem).append(per_mem_graphs);

        var trendlines = {}
        legend.slice(1).map(
            function (_, i) { return i; }
        ).forEach(
            function (i) { trendlines[i] = {} }
        );

        mem_sizes.forEach(function (mem) {
            var new_graph = document.createElement('span');
            var options = {
                title: mem.toString() + ' MB',
                hAxis: { title: 'Time taken in seconds'},
                vAxis: { title: 'Operations'},
                width: 800,
                height: 800,
                trendlines: trendlines,
                chartArea: { width: '60%' },
            };

            var data = google.visualization.arrayToDataTable(
                [
                    legend,
                ].concat(
                    all_mem(
                        mem,
                        results
                    ).map(
                        row_func
                    )
                )
            );
            new google.visualization.ScatterChart(new_graph).draw(data, options);
            $(new_graph).css('float', 'left');

            $(per_mem_graphs).append(new_graph);
        });
        $(per_mem).accordion({
            collapsible: true,
            active: false,
        });
    });
}
