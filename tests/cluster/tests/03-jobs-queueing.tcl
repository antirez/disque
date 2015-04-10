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
    set myjob [lindex [D 0 getjob from $qname] 0]
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
    set myjob [lindex [D 0 getjob from $qname] 0]
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

for {set j 0} {$j < 10} {incr j} {
    set qname [randomQueue]
    set repl_level 3
    set body "xxxyyy$j"
    set target_id [randomInt $::instances_count]
    test "Job replicated to N nodes is delivered after crashing N-1 nodes #$j" {
        set id [D $target_id addjob $qname $body 5000 replicate $repl_level retry 1]
        set job [D $target_id show $id]
        assert {$id ne {}}

        # Kill 3 random nodes.
        set to_kill [expr {$repl_level-1}]
        set killed {}
        while {$to_kill > 0} {
            set kill_id [randomInt $::instances_count]
            if {[lsearch -exact $killed $kill_id] != -1} continue
            kill_instance disque $kill_id
            lappend killed $kill_id
            incr to_kill -1
        }

        # Wait for the job to be re-queued, in case we killed the instance
        # where the job was previously queued.
        wait_for_condition {
            [count_job_copies $job queued] > 0
        } else {
            fail "Job not requeued after some time"
        }

        # Verify it's actually our dear job
        set queueing_id [lindex [get_job_instances $job queued] 0]
        assert {$queueing_id ne {}}
        set myjob [lindex [D $queueing_id getjob from $qname] 0]
        assert {[lindex $myjob 0] eq $qname}
        assert {[lindex $myjob 1] eq $id}
        assert {[lindex $myjob 2] eq $body}

        # Restart nodes
        foreach node_id $killed {
            restart_instance disque $node_id
        }
    }
}

test "Single node jobs are correctly ordered in a FIFO fashion" {
    set qname [randomQueue]
    for {set j 0} {$j < 100} {incr j} {
        D 0 addjob $qname $j 5000 replicate 1 retry 0
    }
    for {set j 0} {$j < 100} {incr j} {
        set job [D 0 getjob from $qname TIMEOUT 5000]
        set body [lindex $job 0 2]
        assert {$body == $j}
    }
}
