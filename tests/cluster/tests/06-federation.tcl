source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

set numjobs 100

test "Add multiple jobs to the same queue, different nodes" {
    for {set j 0} {$j < $numjobs} {incr j} {
        set i [randomInt $::instances_count]
        set id [D $i addjob myqueue job_$j 5000 replicate 3]
        set job [D $i show $id]
        assert {$id ne {}}
        assert {[count_job_copies $job active] >= 2}
    }
}

test "Make sure node 0 does not have all the jobs queued" {
    assert {[D 0 qlen myqueue] != $numjobs}
}

test "I should be able to fetch each job from instance 0" {
    set jobs {}
    while {[llength [lsort -unique $jobs]] != $numjobs} {
        set myjob [D 0 getjob from myqueue TIMEOUT 5000]
        lappend jobs [lindex $myjob 0 2]
        D 0 ackjob [lindex $myjob 0 1]
    }
}
