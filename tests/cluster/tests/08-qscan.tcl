source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"


test "QSCAN based on non blocking iterator" {
    # Create 512 queues, each has a job enqueued
    for {set i 0} {$i < 512} {incr i} {
        set qname [randomQueue]
        lappend myqueues $qname
        D 0 addjob $qname myjob 5000 replicate 1 retry 0
    }
    set cur 0
    set queues {}
    while 1 {
        set res [D 0 qscan $cur]
        set cur [lindex $res 0]
        set que [lindex $res 1]
        lappend queues {*}$que
        if {$cur == 0} break
    }
    # Eliminate duplicates
    set queues [lsort -unique $queues]
    assert_equal 512 [llength $queues]
    assert_equal [lsort $queues] [lsort $myqueues]
    # Remove all the jobs, queues
    D 0 debug flushall
}

test "QSCAN with BUSYLOOP option" {
    # Create 512 queues, each has a job enqueued
    for {set i 0} {$i < 512} {incr i} {
        D 0 addjob [randomQueue] myjob 5000 replicate 1 retry 0
    }
    set res [D 0 qscan busyloop]
    set queues [lindex $res 1]
    assert_equal 512 [llength $queues]
    # Remove all the jobs, queues
    D 0 debug flushall
}

test "QSCAN with MINLEN or MAXLEN option" {
    # Create 512 queues, each has a job enqueued
    for {set i 0} {$i < 512} {incr i} {
        D 0 addjob [randomQueue] myjob 5000 replicate 1 retry 0
    }
    # Get all queue names by qscan busyloop
    set res [D 0 qscan busyloop]
    set queues [lindex $res 1]

    # Add a job to each of the first 16 queues
    for {set i 0} {$i < 16} {incr i} {
        D 0 addjob [lindex $queues $i] myjob 5000 replicate 1 retry 0
    }
    set res [D 0 qscan busyloop minlen 2]
    set queues [lindex $res 1]
    assert_equal 16 [llength $queues]

    # Add a job to each of the first 8 queues
    for {set i 0} {$i < 8} {incr i} {
        D 0 addjob [lindex $queues $i] myjob 5000 replicate 1 retry 0
    }
    set res [D 0 qscan busyloop minlen 3]
    set queues [lindex $res 1]
    assert_equal 8 [llength $queues]

    set res [D 0 qscan busyloop minlen 2 maxlen 3]
    set queues [lindex $res 1]
    assert_equal 16 [llength $queues]
    # Remove all the jobs, queues
    D 0 debug flushall
}

test "QSCAN with IMPORTRATE option" {
    # Use Node-1 as producer and Node-2 as consumer
    for {set i 0} {$i < 4} {incr i} {
        D 1 addjob queue_0001 myjob 0
        D 1 addjob queue_0002 myjob 0
        D 1 addjob queue_0003 myjob 0
        D 1 addjob queue_0004 myjob 0
    }

    for {set i 0} {$i < 4} {incr i} {
        D 2 getjob from queue_0001
        D 2 getjob from queue_0002
        D 2 getjob from queue_0003
        D 2 getjob from queue_0004
    }

    set res [D 2 qscan busyloop importrate 1]
    set queues [lindex $res 1]
    assert_equal 4 [llength $queues]
}
