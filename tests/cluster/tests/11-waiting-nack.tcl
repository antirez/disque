source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "WORKING returns the retry time for the job" {
    set qname [randomQueue]
    set id [D 0 addjob $qname myjob 5000 replicate 3 retry 123]
    assert {[D 0 working $id] == 123}
    D 0 ackjob $id
}

test "WORKING can prevent the job to be requeued (without partitions)" {
    set qname [randomQueue]
    set id [D 0 addjob $qname myjob 5000 replicate 3 retry 3]
    set job [D 0 show $id]
    assert {$id ne {}}
    set myjob [lindex [D 0 getjob from $qname] 0]
    assert {[lindex $myjob 0] eq $qname}
    assert {[lindex $myjob 1] eq $id}
    assert {[lindex $myjob 2] eq "myjob"}

    set now [clock seconds]
    while {([clock seconds]-$now) < 10} {
        assert {[count_job_copies $job queued] == 0}
        D 0 working $id
        after 1000
    }
}
