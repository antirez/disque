source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "Enable AOF" {
    foreach_disque_id j {
        D $j CONFIG SET appendonly yes
        D $j CONFIG REWRITE
        wait_for_condition {
            [DI $j aof_state] eq {on}
        } else {
            fail "Can't enable AOF for instance #$j"
        }
    }
}

for {set i 0} {$i < 5} {incr i} {
    set qname [randomQueue]
    set repl_level 3
    set body "xxxyyy$j"
    set target_id [randomInt $::instances_count]
    test "Job replicated to N nodes is delivered after mass restart #$i" {
        set id [D $target_id addjob $qname $body 5000 replicate $repl_level retry 1]
        set job [D $target_id show $id]
        assert {$id ne {}}

        # Kill all nodes.
        foreach_disque_id j {
            kill_instance disque $j
        }

        # Restart them all.
        foreach_disque_id j {
            restart_instance disque $j
        }

        # Wait for the job to be re-queued after restart.
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
    }
}

test "Disable AOF" {
    foreach_disque_id j {
        D $j CONFIG SET appendonly no
        D $j CONFIG REWRITE
    }
}

