source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "JSCAN based on non blocking iterator" {
    set myjobs {}

    # Create 512 queues, each has a job enqueued
    for {set i 0} {$i < 512} {incr i} {
        set jobid [D 0 addjob [randomQueue] myjob-$i 5000 replicate 1 retry 0]
        lappend myjobs $jobid
    }
    set cur 0
    set jobs {}
    while 1 {
        set res [D 0 jscan $cur]
        set cur [lindex $res 0]
        set job [lindex $res 1]
        lappend jobs {*}$job
        if {$cur == 0} break
    }

    # Eliminate duplicates
    set jobs [lsort -unique $jobs]
    assert_equal 512 [llength $jobs]
    assert_equal [lsort $jobs] [lsort $myjobs]

    # Iterate again to see if all the job bodies are actaully returned.
    set cur 0
    set jobs {}
    while 1 {
        set res [D 0 jscan $cur reply all]
        set cur [lindex $res 0]
        set job [lindex $res 1]
        lappend jobs {*}$job
        if {$cur == 0} break
    }

    # Accumulate unique bodies and check if they are the right format
    # and amount.
    set bodies {}
    foreach j $jobs {
        set body [dict get $j body]
        assert_match {myjob-*} $body
        lappend bodies $body
    }
    set bodies [lsort -unique $bodies]
    assert_equal 512 [llength $bodies]

    # Remove all the jobs, queues
    D 0 debug flushall
}

test "JSCAN with BUSYLOOP option" {
    # Create 512 queues, each has a job enqueued
    for {set i 0} {$i < 512} {incr i} {
        D 0 addjob [randomQueue] myjob 5000 replicate 1 retry 0
    }
    set res [D 0 jscan busyloop]
    set jobs [lindex $res 1]
    assert_equal 512 [llength $jobs]

    # Remove all the jobs, queues
    D 0 debug flushall
}

test "JSCAN with QUEUE option" {
    set queue1 [randomQueue]
    set queue2 [randomQueue]

    # Create jobs
    D 0 addjob $queue1 myjob 5000 replicate 1 retry 0
    set id [D 0 addjob $queue2 myjob 5000 replicate 1 retry 0]

    # Get all jobs using busyloop
    set res [D 0 jscan queue $queue2 busyloop]
    set jobs [lindex $res 1]
    assert_equal 1 [llength $jobs]
    assert_equal $id [lindex $jobs 0]

    # Remove all the jobs, queues
    D 0 debug flushall
}

test "JSCAN with STATE option" {
    # Create jobs
    set id [D 0 addjob myqueue myjob 5000 replicate 1 retry 0]
    D 0 addjob myqueue myjob 5000 replicate 1 retry 0

    # Fetch job
    D 0 getjob from myqueue

    # Get all jobs 
    set res [D 0 jscan state active busyloop]
    set jobs [lindex $res 1]
    assert_equal 1 [llength $jobs]
    assert_equal $id [lindex $jobs 0]

    # Remove all the jobs, queues
    D 0 debug flushall
}

test "JSCAN with REPLY ALL option" {
    # Create job
    set id [D 0 addjob myqueue myjob 5000 replicate 1 retry 0]

    # Get jobs using ALL
    set res [D 0 jscan reply all busyloop]

    # Get first job
    set job [lindex [lindex $res 1] 0]
    assert_equal myqueue [dict get $job queue]
    assert_equal $id [dict get $job id]
    assert_equal myjob [dict get $job body]

    # Remove all the jobs, queues
    D 0 debug flushall
}
