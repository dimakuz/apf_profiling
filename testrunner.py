#!/usr/bin/python
import datetime
import functools
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import libvirt
import pymongo
import minimemslap as mms

DEVNULL = '/dev/null'
IP_PREFIX = '192.168.222.'
MAC_PREFIX = '52:54:00:33:44:'
CGROUP_MEM_FMT = (
    '/sys/fs/cgroup/memory/machine.slice/'
    'machine-qemu\\x2d%s.scope'
)


def logged(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(
            'Calling %s with %s, %s',
            func.func_name,
            repr(args),
            repr(kwargs)
        )
        return func(*args, **kwargs)


def get_db(name):
    return pymongo.MongoClient()['apf'][name]

ip = lambda n: '%s%d' % (IP_PREFIX, n)
ip_suffix = lambda x: int(x.split('.')[-1])
mac = lambda n: '%s%02x' % (MAC_PREFIX, n)
ip_to_mac = lambda ip: mac(int(ip.split('.')[-1]))


_name_to_ip = get_db('ip-vm_name')


def alloc_ip(name):
    allocated_suffixes = [ip_suffix(row['ip']) for row in _name_to_ip.find()]
    sfx = (set(allocated_suffixes) ^ set(range(2, 220))).pop()
    _name_to_ip.insert({'ip': ip(sfx), 'vm_name': name})
    return ip(sfx)


def free_ip(name):
    _name_to_ip.remove({'vm_name': name})

_libvirt_con = None


def get_libvirt_conn():
    global _libvirt_con
    if _libvirt_con is None:
        _libvirt_con = libvirt.open()
    return _libvirt_con


def get_rand_name():
    return str(uuid.uuid4())[:8]


def create_image_from_template(template_path, output_path):
    with open(DEVNULL, 'w') as f:
        subprocess.check_call(
            [
                "qemu-img",
                "create",
                "-f", "qcow2",
                "-b", os.path.abspath(template_path),
                output_path,
            ],
            stdout=f,
            stderr=f,
        )
    os.chmod(output_path, 0o777)


class Timer:
    def __init__(self, timeout=None):
        self._timeout = timeout

    def __enter__(self):
        self._start_time = time.time()
        return self

    def __exit__(self, *a, **kw):
        self.total_time = self.elapsed()

    def expired(self):
        if self._timeout:
            return time.time() > (self._start_time + self._timeout)
        return False

    def elapsed(self):
        return time.time() - self._start_time


class TestVM:
    def __init__(self, template_path, mem_size, name=None, ip=None):
        self._template_path = template_path
        self._mem_size = mem_size
        self.name = name and name or get_rand_name()
        self.ip = ip
        self.ssh_history = []

    def _provision(self):
        disk_path = os.path.join(self.prefix, 'disk.img')
        create_image_from_template(self._template_path, disk_path)

        if self.ip is None:
            self.ip = alloc_ip(self.name)

        with open('dom_template.xml') as f:
            template = f.read()

        subs = {
            'name': self.name,
            'mem_size': self._mem_size,
            'disk_path': disk_path,
            'mac_addr': ip_to_mac(self.ip),
        }
        for k, v in subs.items():
            template = template.replace('@%s@' % (k.upper()), str(v))

        get_libvirt_conn().createXML(template)

    def set_cgroup_memory_limit(self, limit_in_mbytes):
        path = os.path.join(
            CGROUP_MEM_FMT % self.name,
            'memory.limit_in_bytes',
        )
        limit_in_bytes = str(limit_in_mbytes * (2 ** 20))

        while True:
            try:
                with open(path, 'w') as f:
                    f.write(limit_in_bytes)
                break
            except IOError:
                continue

    def _wait_for_ssh(self, timeout=60):
        with Timer(timeout) as timer:
            while not timer.expired():
                ret = self.ssh(['true'])
                if ret[0] == 0:
                    self.ssh_history = []
                    return
            raise RuntimeError('Remote shell unavailable')

    def ssh(self, command, background=False):
        ssh_command = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "root@%s" % (self.ip),
        ] + command

        if background:
            ssh_command.insert(1, '-f')

        with open(DEVNULL, 'w') as f:
            proc = subprocess.Popen(
                ssh_command,
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            self.ssh_history.append({
                'command': command,
                'ssh_command': ssh_command,
                'background': background,
                'return_code': proc.returncode,
                'stdout': out,
                'stderr': err,
            })
            return (proc.returncode, out, err)

    def _scp(self, path1, path2):
        command = [
            "scp",
            "-q",
            "-o", "StrictHostKeyChecking no",
            "-o", "UserKnownHostsFile=/dev/null",
            path1,
            path2,
        ]
        with open(DEVNULL, 'w') as f:
            return subprocess.call(
                command,
                stdin=f, stdout=f, stderr=f
            )

    def scp_from(self, remote_path, local_path):
        return self._scp(
            "root@%s:%s" % (self.ip, remote_path),
            local_path,
        )

    def scp_to(self, local_path, remote_path):
        return self._scp(
            local_path,
            "root@%s:%s" % (self.ip, remote_path),
        )

    def _destroy(self):
        free_ip(self.name)
        get_libvirt_conn().lookupByName(self.name).destroy()

    def __enter__(self):
        self.prefix = tempfile.mkdtemp(prefix='/home/shared/',
                                       suffix='-%s' % self.name)
        try:
            os.chmod(self.prefix, 0o777)
            self._provision()
            try:
                self._wait_for_ssh()
            except:
                self._destroy()
                raise
        except:
            shutil.rmtree(self.prefix)
            raise
        return self

    def __exit__(self, type, value, tb):
        self._destroy()
        shutil.rmtree(self.prefix)


def apache_test(vm, requests, concurrency):
    vm.ssh('sysctl -w vm.swappiness=0'.split(' '))
    print 'Running ab'
    url = 'http://%s/index2.php' % (vm.ip)
    proc = subprocess.Popen(
        [
            'ab',
            '-k',
            '-n', str(requests),
            '-c', str(concurrency),
            url
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    print 'done ab'
    return {
        'exitcode': proc.returncode,
        'stdout': out,
        'stderr': err
    }


def memcached_setup(vm, count, value_size):
    mms.populate(vm.ip, count, value_size)


def memcached_test_mini(vm, count, key_limit, concurrency):
    vm.ssh('sysctl -w vm.swappiness=0'.split(' '))
    print 'Running mini-memslap'
    start = time.time()
    mms.parallel_slap(vm.ip, count, key_limit, concurrency)
    total = time.time() - start
    return total


def collect_files(vm, files):
    records = {}
    for path in files:
        try:
            local_path = os.path.join(vm.prefix, 'tmp')
            vm.scp_from(path, local_path)
            with open(local_path) as f:
                records[path] = f.read()
            os.unlink(local_path)
        except IOError:
            print 'failed to copy %s' % path
    return records


@logged
def run_test(test, machine_spec,
             cgroup_limit=None,
             perf=None,
             files=None,
             files_pre=None,
             files_post=None,
             host_files=None,
             tags=[]):

    files = files or []
    files_pre = files_pre or []
    files_post = files_post or []

    host_files = host_files or []

    files_pre += files
    files_post += files

    with TestVM(**machine_spec) as vm:
        print 'VM is up'
        files_pre_records = collect_files(vm, files_pre)
        host_files_pre_records = []
        for path in host_files:
            with open(path) as f:
                host_files_pre_records.append(
                    {
                        'path': path,
                        'contents': f.read(),
                    },
                )
        print 'Pre-files collected'

        # FIXME:
        vm.ssh('systemctl restart systemd-sysctl'.split(' '))
        if 'setup' in test:
            test['setup'](vm)
        if cgroup_limit:
            vm.set_cgroup_memory_limit(cgroup_limit)
        if perf:
            perf_command = ["perf", "record", "-a", "-g"]
            if 'user' in perf:
                perf_command.extend(["-u", perf['user']])
            for event in perf.get('events', []):
                perf_command.extend(["-e", event])
            vm.ssh([
                'nohup %s 1>/dev/null 2>/dev/null &' % (' '.join(perf_command))
            ])

        duration = result = None
        try:
            with Timer() as timer:
                print 'run test'
                result = test['func'](vm,
                                      *test.get('args', []),
                                      **test.get('kwargs', {}))
                print 'test done'
                duration = timer.elapsed()

        finally:
            if cgroup_limit:
                vm.set_cgroup_memory_limit(4096)

            # vm.ssh(['dmesg'])
            # print 'done dmesg'

            if perf:
                vm.ssh('pkill -SIGTERM perf'.split(' '))
                while vm.ssh('pgrep ^perf'.split(' '))[0] == 0:
                    continue
                vm.ssh(
                    ['perf script -i /root/perf.data | tee /tmp/perf.script']
                )

                perf_local_path = os.path.join(vm.prefix, 'perf.script')
                vm.scp_from('/tmp/perf.script', perf_local_path)
                with open(perf_local_path) as f:
                    perf['output'] = f.read()
            files_post_records = collect_files(vm, files_post)
            host_files_post_records = []
            for path in host_files:
                with open(path) as f:
                    host_files_post_records.append(
                        {
                            'path': path,
                            'contents': f.read(),
                        },
                    )
            vm.ssh(['uname', '-a'])

            record = {
                'test': {
                    'name': test['func'].__name__,
                    'result': result,
                    'args': test.get('args', []),
                    'kwargs': test.get('kwargs', {}),
                },
                'timestamp': str(datetime.datetime.now()),
                'duration': duration,
                'machine_spec': machine_spec,
                'cgroup_limit': cgroup_limit,
                'perf': perf,
                'tags': tags,
                'ssh_history': vm.ssh_history,
                'files': {
                    'host': {
                        'pre': host_files_pre_records,
                        'post': host_files_post_records,
                    },
                    'guest': {
                        'pre': files_pre_records,
                        'post': files_post_records,
                    },
                },
            }
            open('/tmp/foobar', 'w').write(repr(record))
            get_db('results').insert(record)
            return record

MEM_SIZES = (256, 277, 298, 320, 341, 362, 384, 512, 1024, 2048)
GUEST_FILES = [
    '/sys/block/dm-0/stat',  # root
    '/sys/block/dm-1/stat',  # swap
    '/sys/block/dm-2/stat',  # rand-files
    '/sys/block/dm-3/stat',  # rand-files - occasional
]
HOST_FILES = ['/sys/block/dm-1/stat']
APACHE_TEST = {
    'func': apache_test,
    'kwargs': {
        'requests': 1000 * 1000,
        'concurrency': 75,
        'times': 20,
    },
}
APACHE_USER = 'apache'
MEMCACHED_TEST = {
    'setup': functools.partial(
        memcached_setup,
        count=2**16,
        value_size=2**12
    ),
    'func': memcached_test_mini,
    'kwargs': {
        'count': 50 * 1000,
        'key_limit': 2 ** 16,
        'concurrency': 10,
    },
}
MEMCACHED_USER = 'memcached'

TEMPLATE_CLEAN = '/home/shared/fedora20-clean.qcow2.template'
TEMPLATE_FIX = '/home/shared/fedora20-withfix3.qcow2.template'
TEMPLATE_NOFIX = '/home/shared/fedora20-withoutfix3.qcow2.template'

ITERS = 10


def main(args):
    # do 'optimum' runs
    for mem_size in MEM_SIZES:
        for i in range(ITERS):
            run_test(
                machine_spec={
                    'template_path': TEMPLATE_CLEAN,
                    'mem_size': mem_size,
                },
                setup=functools.partial(
                    memcached_setup,
                    count=2**16,
                    value_size=2**12
                ),
                test=MEMCACHED_TEST,
                files=GUEST_FILES,
                host_files=HOST_FILES,
                tags=['%d/%d' % (i, ITERS)]
            )

    for cgroup_limit in MEM_SIZES:
        for i in range(ITERS):
            for template in (
                    TEMPLATE_FIX,
                    TEMPLATE_NOFIX,
            ):
                while True:
                    try:
                        run_test(
                            machine_spec={
                                'template_path': template,
                                'mem_size': 2048,
                            },
                            test=MEMCACHED_TEST,
                            cgroup_limit=cgroup_limit,
                            perf={
                                'events': ['sched:kvm_will_halt'],
                                'user': MEMCACHED_USER,
                            },
                            files=GUEST_FILES,
                            host_files=HOST_FILES,
                            tags=['%d/%d' % (i, ITERS)]
                        )
                        break
                    except Exception:
                        print 'retrying'
                        continue


if __name__ == '__main__':
    main(sys.argv[1:])
