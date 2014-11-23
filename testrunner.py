#!/usr/bin/python
import base64
import contextlib
import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zlib

import libvirt
import pymongo

DEVNULL = '/dev/null'
IP_PREFIX = '192.168.222.'
MAC_PREFIX = '52:54:00:33:44:'
CGROUP_MEM_FMT = (
    '/sys/fs/cgroup/memory/machine.slice/'
    'machine-qemu\\x2d%s.scope'
)


_db = None
def get_db():
    global _db
    if _db is None:
        con = pymongo.MongoClient()
        _db = con['apf']
    return _db


ip = lambda n: '%s%d' % (IP_PREFIX, n)
ip_suffix = lambda x: int(x.split('.')[-1])
mac = lambda n: '%s%02x' % (MAC_PREFIX, n)
ip_to_mac = lambda ip: mac(int(ip.split('.')[-1]))


_name_to_ip = get_db()['ip-vm_name']
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
        for k,v in subs.items():
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
                if ret == 0:
                    return
            raise RuntimeError('Remote shell unavailable')

    def ssh(self, command, background=False):
        ssh_command = [
            "ssh",
            "-o", "StrictHostKeyChecking no",
            "root@%s" % (self.ip),
        ] + command

        if background:
            ssh_command.insert(1, '-f')

        with open(DEVNULL, 'w') as f:
            return subprocess.call(
                ssh_command,
                stdin=f, stdout=f, stderr=f,
            )

    def _scp(self, path1, path2):
        command = [
            "scp",
            "-o", "StrictHostKeyChecking no",
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


def simple_test(vm):
    print vm.name, vm.ip, vm.prefix
    vm.ssh(['uname -r | tee /tmp/uname'])
    vm.scp_from('/tmp/uname', '/tmp/uname')


def apache_test(vm, requests, concurrency):
    url = 'http://%s/index2.php' % vm.ip
    subprocess.call([
        'ab', '-k', '-r',
        '-n', str(requests),
        '-c', str(concurrency),
        url
    ])


def run_test(machine_spec, test_fn, test_args=[], test_kwargs={},
             cgroup_limit=None, perf_events=None, perf_user=None):
    with TestVM(**machine_spec) as vm:
        if cgroup_limit:
            vm.set_cgroup_memory_limit(cgroup_limit)
        if perf_events:
            perf_command = ["perf", "record", "-a", "-g"]
            if perf_user:
                perf_command.extend(["-u", perf_user])
            for event in perf_events:
                perf_command.extend(["-e", event])
            # vm.ssh(perf_command, background=True)
            vm.ssh([
                'nohup %s 1>/dev/null 2>/dev/null &' % (' '.join(perf_command))
            ])

        duration = result = None
        try:
            with Timer() as timer:
                result = test_fn(vm, *test_args, **test_kwargs)
                duration = timer.elapsed()

        finally:
            if cgroup_limit:
                vm.set_cgroup_memory_limit(4096)

            if perf_events:
                vm.ssh('pkill -SIGTERM perf'.split(' '))
                while vm.ssh('pgrep ^perf'.split(' ')) == 0:
                    continue
                vm.ssh(['perf script -i /root/perf.data > /tmp/perf.script'])

                perf_local_path = os.path.join(vm.prefix, 'perf.script')
                vm.scp_from('/tmp/perf.script', perf_local_path)
                with open(perf_local_path) as f:
                    output = f.read()
                    perf_out = base64.b64encode(zlib.compress(output, 9))
            record = {
                'type': test_fn.__name__,
                'timestamp': str(datetime.datetime.now()),
                'test_params': (test_args, test_kwargs),
                'duration': duration,
                'result': result,
                'machine_spec': machine_spec,
                'cgroup_limit': cgroup_limit,
                'perf': {
                    'events': perf_events,
                    'user': perf_user,
                    'output': perf_events and perf_out or None
                },
            }
            get_db()['results'].insert(record)


def main(args):
    for _ in range(50):
        run_test(
            machine_spec={
                'template_path': args[0],
                'mem_size': 384, #  1024,
            },
            test_fn=apache_test,
            test_kwargs={
                'requests': 100 * 1000,
                'concurrency': 10,
            },
            cgroup_limit=None, # 386,
            perf_events=[], # ['sched:kvm_will_halt'],
            perf_user='apache',
        )


if __name__ == '__main__':
    main(sys.argv[1:])
