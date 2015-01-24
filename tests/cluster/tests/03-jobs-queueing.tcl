source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "ADDJOB will initiall only queue the job in the target node" {
    set id [D 0 addjob myqueue myjob 5000 replicate 3]
    set job [D 0 show $id]
    assert {$id ne {}}
    assert {[count_job_copies $job active] == 2}
}

test "If the job is not consumed, without partitions, no requeue happens" {
    set id [D 0 addjob myqueue myjob 5000 replicate 3 retry 1]
    set job [D 0 show $id]
    assert {$id ne {}}
    for {set j 0} {$j < 3} {incr j} {
        assert {[count_job_copies $job queued] == 1}
        after 1000
    }
}

test "If the job is consumed, but not acknowledged, it gets requeued" {
    set qname [randomQueue]
    set id [D 0 addjob $qname myjob 5000 replicate 3 retry 1]
    set job [D 0 show $id]
    assert {$id ne {}}
    set myjob [lindex [D 0 getjobs from $qname] 0]
    assert {[lindex $myjob 0] eq $qname}
    assert {[lindex $myjob 1] eq $id}
    assert {[lindex $myjob 2] eq "myjob"}

    wait_for_condition {
        [count_job_copies $job queued] > 0
    } else {
        fail "Job not requeued after some time"
    }
}

test "If retry is set to 0, no requeue happens after a job is consumed" {
    set qname [randomQueue]
    set id [D 0 addjob $qname myjob 5000 replicate 1 retry 0]
    set job [D 0 show $id]
    assert {$id ne {}}
    set myjob [lindex [D 0 getjobs from $qname] 0]
    assert {[lindex $myjob 0] eq $qname}
    assert {[lindex $myjob 1] eq $id}
    assert {[lindex $myjob 2] eq "myjob"}
    after 2000 ; # Enough time to, potentially, requeue.
    assert {[count_job_copies $job queued] == 0}
}

test "If the job is not consumed, but queueing node unreachable, is requeued" {
    set id [D 0 addjob myqueue myjob 5000 replicate 3 retry 1]
    set job [D 0 show $id]
    assert {$id ne {}}
    assert {[count_job_copies $job queued] == 1}
    kill_instance disque 0
    wait_for_condition {
        [count_job_copies $job queued] > 0
    } else {
        fail "Job not requeued after some time"
    }
    restart_instance disque 0
}
