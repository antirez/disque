source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "Acks work in the single node case" {
    set id [D 0 addjob myqueue myjob 0 replicate 1]
    assert {[D 0 show $id] ne {}}
    D 0 ackjob $id
    assert {[D 0 show $id] eq {}}
}

test "Acks are propagated in the cluster" {
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    assert {[count_job_copies $job {active queued}] >= 5}
    D 0 ackjob $id
    wait_for_condition {
        [count_job_copies $job {active queued}] == 0
    } else {
        fail "ACK garbage collection failed"
    }
}

test "Acks are evicted only if all the job owners can be reached" {
    # Add job to 5 nodes
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    set instances [get_job_instances $job {active queued}]

    # Kill originating node
    kill_instance disque 0

    # Find the first instance having a copy different than instance 0.
    foreach ownerid $instances {
        if {$ownerid != 0} break
    }

    # Acknowledged the job
    D $ownerid ackjob $id

    # Make sure the ack is not deleted
    after 2000
    wait_for_condition {
        [count_job_copies $job {acked}] >= 4
    } else {
        fail "Not enough copies of the acked job foudn"
    }

    # Restart initial node
    restart_instance disque 0

    # Make sure the ack is collected at this point
    wait_for_condition {
        [count_job_copies $job {acked}] == 0
    } else {
        fail "Ack not GCed after some time"
    }
}

test "It is possible to acknowledge jobs to nodes not knowing it" {
    # Add job to 5 nodes
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    set no_copy_instance [lindex [get_job_instances $job {}] 0]

    # Make sure the instance actually does not know about the job
    assert {[D $no_copy_instance show $id] eq {}}

    # Acknowledge the job to an instance not having a copy.
    D $no_copy_instance ackjob $id

    # Make sure the ack is collected at this point
    wait_for_condition {
        [count_job_copies $job {acked}] == 0
    } else {
        fail "Ack not GCed after some time"
    }
}

test "Acknowledge to non owner, while some owner is down" {
    # Add job to 5 nodes
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    set owners [lindex [get_job_instances $job {active queued}] 0]
    set no_copy_instance [lindex [get_job_instances $job {}] 0]

    # Make sure the instance actually does not know about the job
    assert {[D $no_copy_instance show $id] eq {}}

    # Crash the first of the owners
    kill_instance disque [lindex $owners 0]

    # Acknowledge the job to an instance not having a copy.
    D $no_copy_instance ackjob $id

    # Restart the first of the owners
    restart_instance disque [lindex $owners 0]

    # Make sure the ack is collected at this point
    wait_for_condition {
        [count_job_copies $job {acked}] == 0
    } else {
        fail "Ack not GCed after some time"
    }
}

test "Acknowledge to non owner, while all the owners are down" {
    # Add job to 5 nodes
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    set owners [lindex [get_job_instances $job {active queued}] 0]
    set no_copy_instance [lindex [get_job_instances $job {}] 0]

    # Make sure the instance actually does not know about the job
    assert {[D $no_copy_instance show $id] eq {}}

    # Crash all the owners
    foreach ownerid $owners {
        kill_instance disque $ownerid
    }

    # Acknowledge the job to an instance not having a copy.
    D $no_copy_instance ackjob $id

    # Restart all the owners
    foreach ownerid $owners {
        restart_instance disque $ownerid
    }

    # Make sure the ack is collected at this point
    wait_for_condition {
        [count_job_copies $job {acked}] == 0
    } else {
        fail "Ack not GCed after some time"
    }
}

test "Fast ack to owner node" {
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    assert {[count_job_copies $job {active queued}] >= 5}
    D 0 fastack $id
    wait_for_condition {
        [count_job_copies $job {active queued}] == 0
    } else {
        fail "ACK garbage collection failed"
    }
}

test "Fast ack to non owner node" {
    # Add job to 5 nodes
    set id [D 0 addjob myqueue myjob 0 replicate 5]
    set job [D 0 show $id]
    set no_copy_instance [lindex [get_job_instances $job {}] 0]

    # Make sure the instance actually does not know about the job
    assert {[D $no_copy_instance show $id] eq {}}

    # Acknowledge the job to an instance not having a copy.
    D $no_copy_instance fastack $id

    # DELJOB should be broadcasted cluster-wide causign the job to be
    # collected, given that the net is well connected right now.
    wait_for_condition {
        [count_job_copies $job {acked}] == 0
    } else {
        fail "Ack not GCed after some time"
    }
}

