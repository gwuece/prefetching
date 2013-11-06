#!/usr/bin/env python
import commands, subprocess, os, sys, signal, time, re


prefetch_device_map = { 'sdc' : ('/dev/loop0', '/mnt/loop0', '/mnt/sdc1/tmp/span', '/dev/sdc1', 5*250e6),
                        'sdd' : ('/dev/loop1', '/mnt/loop1', '/mnt/sdd1/tmp/span', '/dev/sdd', 5*120e6),
                        'hdd' : ('/dev/loop2', '/mnt/loop2', '/home/ahsen/tmp/span', '/dev/sda', 5*120e6),
                        'sdd1' : ('/dev/loop3', '/mnt/loop3', '/mnt/sdd1/tmp/span', '/dev/sdd', 5*120e6),
                        'md0' : ('/dev/loop4', '/mnt/loop4', '/mnt/md0/tmp/span', '/dev/md_d0', 5*120e6),
                        'ocz0' : ('/dev/loop5', '/mnt/loop5', '/mnt/ocz0/tmp/span', '/dev/sdd1', 5*120e6),
                        'ocz-raid' : ('/dev/loop6', '/mnt/loop6', '/mnt/ocz-raid/tmp/span', '/dev/md1', 5*120e6)}

# dd benchmarks from dd if=span bs=1M
# ocz0: 156 MB/s
# ocz-raid: 174 MB/s
# intel-raid:

def run_dropcache():
    cmd_dropcache = 'sync && echo 3 > /proc/sys/vm/drop_caches'
    print 'Drop caches'
    (rc, out) = commands.getstatusoutput(cmd_dropcache)
    if rc:
        print 'Error executing %s' % cmd_dropcache


def run_prefetchd(interval, scale, readahead_interval, low_readahead_time, beta, alpha, adaptive, red_block_threshold, max_throughput, loop_dev, span_file):
    subprocess.Popen( \
                        'INTERVAL=%g SCALE=%g READAHEAD_TIME=%g LOW_READAHEAD_TIME=%g RATIO_BETA=%g RATIO_ALPHA=%g PREFETCH_ADAPTIVE=%d RED_BLOCK_THRESHOLD=%g MAX_THROUGHPUT=%g /home/ron/ahsen/prefetchd/src/prefetchd %s %s 0' \
                        % (interval, scale, readahead_interval, low_readahead_time, beta, alpha, adaptive, red_block_threshold, max_throughput, loop_dev, span_file), shell=True)
    print 'Run prefetchd'
    time.sleep(1)

def stop_prefetchd():
    commands.getstatusoutput('sleep 5; killall -INT prefetchd; sleep 3')


def bench_slowcat((mnt_point, rate)):
    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" /home/ron/ahsen/prefetchd/src/slowcat %s/iozone-1G %s 0 1048576 > /dev/null' % (mnt_point, rate)
    run_dropcache()
    (rc, out) = commands.getstatusoutput(cmd)
    elapsed_re = re.compile('(.*)elapsed')
    m = elapsed_re.match(out)
    if m:
        print m.group(1)
    return float(m.group(1))



def bench_blastn(mnt_point):
    sys.stdout.write('rm -r %s/postgres && cp -rp /home/ron/ahsen/ncbi-blast-2.2.24+ %s\n' % (mnt_point[0],mnt_point[0]))

    os.system('rm -r %s/postgres && cp -rp /home/ron/ahsen/ncbi-blast-2.2.24+ %s' % (mnt_point[0],mnt_point[0]))
    
    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" /home/ron/biobench/ncbi-blast-2.2.24+/bin/blastn -db %s/ncbi-blast-2.2.24+/db/nt -query /home/ron/biobench/005.blast/input/batch2.fa' % mnt_point

    run_dropcache()
    print cmd
    (rc, out) = commands.getstatusoutput(cmd)
    print out
    elapsed_re = re.compile('(.*)elapsed')
    for l in out.splitlines():
        m = elapsed_re.match(l)
        if m:
            return float(m.group(1))
    raise IOError


def bench_blastp((mnt_point)):
    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" /home/ron/biobench/ncbi-blast-2.2.24+/bin/blastp -db %s/ncbi-blast-2.2.24+/db/nr -query /home/ron/biobench/005.blast/input/seq1.prot.fa' % mnt_point

    run_dropcache()
    print cmd
    (rc, out) = commands.getstatusoutput(cmd)
    print out
    elapsed_re = re.compile('(.*)elapsed')
    for l in out.splitlines():
        m = elapsed_re.match(l)
        if m:
            return float(m.group(1))
    raise IOError



def bench_postmark(mnt_point):
    path='/home/ron/ahsen/postmark/postmark/postmark-1.51'

    postmark_cmd='set size 1048576 2097152\nset read 65536\nset write 65536\nset transactions 20000\nset location %s/tmp\nshow\nrun\nquit\n' % mnt_point

#    postmark_cmd='set size 10485760 20971520\nset read 1048576\nset write 1048576\nset transactions 500\nset location %s/tmp\nshow\nrun\nquit\n' % mnt_point

    f = open('/tmp/postmark.cmd', 'w')
    f.write(postmark_cmd)
    f.close()

    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" %s/postmark < /tmp/postmark.cmd ' % (path)
    run_dropcache()
    (rc, out) = commands.getstatusoutput(cmd)
    print out
    elapsed_re = re.compile('pm>(.*)elapsed.*')
    for l in out.splitlines():
        m = elapsed_re.match(l)
        if m:
            return float(m.group(1))
    raise IOError


def bench_lfs(mnt_point):
    s = 0.0
    n = 5
    for i in range(n):
        cmd='/home/ron/ahsen/LFStest/largefile -i 1024 -r 40 -f 100000 %s/tmp' % (mnt_point)
        run_dropcache()
        (rc, out) = commands.getstatusoutput(cmd)
        found = False
        elapsed_re = re.compile('seq_read[\s]+([\w]+).*')
        for l in out.splitlines():
            m = elapsed_re.match(l)
            if m:
                s += float(m.group(1))
                found = True
                break
        if not found:
            print out
            raise IOError
    return s

def bench_dbt3((mnt_point, index)):
#    sys.stdout.write('rm -r %s/ncbi-blast-2.2.24+ && cp -rp /home/ron/ahsen/postgres %s\n' % (mnt_point, mnt_point))
    
#    os.system('rm -r %s/ncbi-blast-2.2.24+ && cp -rp /home/ron/ahsen/postgres %s' % (mnt_point, mnt_point))

    server_cmd = 'su postgres -c "/usr/lib/postgresql/8.3/bin/postgres -D %s/postgres > /dev/null &"' % mnt_point

    cmd = 'su postgres -c \'/usr/bin/time -f "%%eelapsed real %%e user %%U sys %%S pct %%P" /usr/bin/psql -e -d dbt3 -f /home/ron/ahsen/dbt3/dbt3-1.9/src/dbgen/%s.sql\'' % index

    print 'Running %s' % server_cmd
    sys.stdout.flush()

    os.system(server_cmd)
    
    time.sleep(3)

    run_dropcache()
    print 'Running %s' % cmd
    sys.stdout.flush()
    (rc, out) = commands.getstatusoutput(cmd)

    print out

    commands.getstatusoutput('killall postgres')

    elapsed_re = re.compile('(.*)elapsed')
    for l in out.splitlines():
        m = elapsed_re.match(l)
        if m:
            print m.group(1)
            return float(m.group(1))
    raise IOError
 
def bench_websearch((mnt_point, loop_dev, replay_speed, n_processes)):
    print 'n_processes = %s' % n_processes
    re_read_time = re.compile('Pid (.*) (.*) Time spent waiting for reads (.*)')
    read_times = []
    path = '/home/ron/ahsen/replay'
    cmd = 'cd %s; ./replay %s umass/WebSearch1.spc %s %s' % (path, loop_dev, replay_speed, n_processes)
    run_dropcache()
    (rc, out) = commands.getstatusoutput(cmd)
    for i in range(n_processes):
        f = open('%s/replay.%d.out' % (path, i))
        for l in f:
            m = re_read_time.match(l)
            if m:
                read_times.append(float(m.group(3)))
    return sum(read_times) / len(read_times)

def bench_websearch2((mnt_point, loop_dev, replay_speed, n_processes)):
    print 'n_processes = %s' % n_processes
    re_read_time = re.compile('Pid (.*) (.*) Time spent waiting for reads (.*)')
    read_times = []
    path = '/home/ron/ahsen/replay'
    cmd = 'cd %s; ./replay %s umass/WebSearch2.spc %s %s' % (path, loop_dev, replay_speed, n_processes)
    run_dropcache()
    (rc, out) = commands.getstatusoutput(cmd)
    for i in range(n_processes):
        f = open('%s/replay.%d.out' % (path, i))
        for l in f:
            m = re_read_time.match(l)
            if m:
                read_times.append(float(m.group(3)))
    return sum(read_times) / len(read_times)


def bench_slowthink((mnt_point, pct, rate)):
    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" /home/ron/ahsen/prefetchd/src/slowthink %s/iozone-1G %s 0 > /dev/null' % (mnt_point, rate)
    print cmd
    run_dropcache()
    (rc, out) = commands.getstatusoutput(cmd)
    elapsed_re = re.compile('(.*)elapsed')
    m = elapsed_re.match(out)
    if m:
        print m.group(1)
    return float(m.group(1))


def bench_spc2(mnt_point):
    cmd = '/usr/bin/time -f \"%%eelapsed real %%e user %%U sys %%S pct %%P\" /home/ron/ahsen/spc/spc2 /home/ron/ahsen/spc/rates1 %s/tmp/test_file > /dev/null' % (mnt_point)
    run_dropcache()
    print cmd
    (rc, out) = commands.getstatusoutput(cmd)
#    elapsed_re = re.compile('(.*)elapsed.*')
    elapsed_re = re.compile('read_time_total = (.*)')
    for l in out.splitlines():
        print l
        m = elapsed_re.match(l)
        if m:
            return float(m.group(1))
    raise IOError


def bench_gofilebench((name, mnt_point, duration)):
    r = re.compile('(.*): (.*): IO Summary: (.*) ops, (.*) ops/s, \((.*) r/w\), (.*)mb/s, (.*)us cpu/op, (.*)ms latency')
    p1 = subprocess.Popen('/home/ron/ahsen/filebench/go_filebench-1.4.8.fsl.0.7/go_filebench', stdin=subprocess.PIPE, stdout=subprocess.PIPE)
#    cmd = 'load %s\nset $dir=%s/tmp\n\n set $nthreads=1\nrun %s\n' % (name, mnt_point, duration)
#    cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=2\nset $filesize=1048576\nrun %s\n' % (name, mnt_point, duration)
#    cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=1\nrun %s\n' % (name, mnt_point, duration)

    if name == 'videoserver':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=4\nset $numactivevids=1\nset $numpassivevids=3\nrun %s\n' % (name, mnt_point, duration)
    elif name == 'multistreamread':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=1\nrun %s\n' % (name, mnt_point, duration)
    elif name == 'multistreamread-delay':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=4\nrun %s\n' % (name, mnt_point, duration)
    elif name == 'fileserver':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=4\nset $filesize=32k\nset $nfiles=20000\nrun %s\n' % (name, mnt_point, duration) 
    elif name == 'fileserver':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=1\nset $filesize=1000m\nset $nfiles=10\nrun %s\n' % (name, mnt_point, duration)
    elif name == 'oltp':
        cmd = 'load %s\nset $dir=%s/tmp\nset $nthreads=1\nrun %s\n' % (name, mnt_point, duration)

    run_dropcache()
    (out, err) = p1.communicate(cmd)
    print out
    for l in out.splitlines():
        m = r.match(l)
        if m:
            print l
            return 1e6 / float(m.group(4))
    print 'Benchmark failed:'
    print out
    raise IOError('benchmark failed')

def read_histogram():
    print 'read_histogram'
#    devs = ['sdd1', 'hdd']
#    devs = ['sdc', 'sdd1', 'hdd']
#    devs = ['md0']
    devs = ['hdd']

    for n_thr in [1]:
        for replay_speed in [12]:
            for dev in devs:
                (loop_dev, mnt_point, span_file, disk_dev, max_throughput) = prefetch_device_map[dev]

#                benches = []

#                benches = [(bench_slowthink, mnt_point, 0, 40e6, 'slowthink-40e6')]
#                benches += [(bench_blastn, mnt_point, 'blastn'), (bench_blastp, mnt_point, 'blastp')]
#                benches += [(bench_lfs, mnt_point, 'lfs'), (bench_postmark, mnt_point, 'postmark')]

                # benches = [(bench_websearch, mnt_point, loop_dev, replay_speed, n_thr, 'websearch'), (bench_websearch2, mnt_point, loop_dev, replay_speed, n_thr, 'websearch2')]

                 for i in [3, 4, 5, 7, 11, 19]:
                     benches += [(bench_dbt3, mnt_point, i, 'dbt3-%s' % i)]

                # for i in range(22):
                #     benches += [(bench_dbt3, mnt_point, i, 'dbt3-%s' % i)]

                for bench in benches:
                    # baseline

        #            os.system('blktrace %s &' % disk_dev)

                    print (bench[1:-1])

                    r0 = bench[0](bench[1:-1])
        #            r0 = 1.0

                    # os.system('killall blktrace')
                    # time.sleep(2)
                    # os.system('ls -l %s.blktrace.*' % os.path.basename(disk_dev))

                    # trace_file = 'prefetch-base-trace-dev-%s-bench-%s.trace' % (dev, str(bench[-1]))

                    # os.system('blkparse %s -o %s' % (os.path.basename(disk_dev), trace_file))

                    # os.system('/home/ron/ahsen/replay/histogram.py %s' % trace_file)

                    scale = 1.0
                    rd = 0.50
                    #red_block_threshold = 0.05
                    red_block_threshold = 0.00

                    for i in [-1]:
                        if i == -1:
                            print 'normal'
                            scale = 1.0
                            adaptive = 0
                        if i == 0:
                            print 'normal'
                            scale = 0.1
                            adaptive = 0
                        elif i == 1:
                            scale = 8.0
                            adaptive = 0
                            print 'aggressive'
                        elif i == 2:
                            scale = 1.0
                            adaptive = 1
                            print 'adaptive'
                            # adaptive range is 1.0 to 16.0

                        (high_rd, low_rd, beta, alpha) = (rd, rd, .50, 0.90)

                        red_block_threshold = 0.0

                        run_prefetchd(0.50, scale, high_rd, low_rd, beta, alpha, adaptive, red_block_threshold, max_throughput, loop_dev, span_file)
                        try:
        #                    os.system('blktrace %s &' % disk_dev)

                            r1 = bench[0](bench[1:-1])
                            stop_prefetchd()

        #                    os.system('killall blktrace')
        #                    time.sleep(2)
        #                    os.system('ls -l %s.blktrace.*' % os.path.basename(disk_dev))
        ##                    trace_file = 'prefetch-actual-trace-dev-%s-bench-%s.trace' % (dev, str(bench[-1]))
        #                    os.system('blkparse %s -o %s' % (os.path.basename(disk_dev), trace_file))
        #                    print 'Prefetch Histogram:'
        #                    os.system('/home/ron/ahsen/replay/histogram.py %s' % trace_file)

                        except IOError:
                            stop_prefetchd()
                            pass

                        trace_file = 'prefetch-trace-dev-%s-bench-%s-scale-%s-beta-%s-alpha-%s-adaptive-%s.trace25' % (dev, str(bench[-1]), scale, beta, alpha, adaptive)
                        os.system('tail prefetch.trace; cp prefetch.trace %s' % trace_file)
                        print 'Result: adap:%s rd:%s scale:%s bench:%s dev:%s %s %s %s' % (adaptive, rd, scale, str(bench[-1]), dev, r0, r1, r0 / r1)
                        sys.stdout.flush()

    sys.exit(0)


if __name__ == "__main__":
    read_histogram()
