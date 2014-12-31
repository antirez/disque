# Check the basic monitoring and failover capabilities.

source "../tests/includes/init-tests.tcl"

test "ADDJOB, single node" {
    set id [D 0 addjob myqueue myjob 5000 replicate 1]
    set job [D 0 show $id]
    assert {$id ne {}}
    assert {[llength [dict get $job nodes-delivered]] == 1}
    assert {[dict get $job state] eq "queued"}
}

# Count how many copies of the jobs are found among the nodes that
# may have received a copy.
proc count_job_copies {job} {
    set job_id [dict get $job id]
    set delivered [dict get $job nodes-delivered]
    set copies 0
    foreach_disque_id j {
        set node_id [dict get [get_myself $j] id]
        if {[lsearch -exact $delivered $node_id] == -1} continue
        set job [D $j show $job_id]
        if {$job ne {} &&
            ([dict get $job state] eq {queued} ||
             [dict get $job state] eq {active})} {
             incr copies
        }
    }
    return $copies
}

test "ADDJOB, synchronous replication to multiple nodes" {
    set job_id [D 0 addjob myqueue myjob 5000 replicate 3]
    assert {$job_id ne {}}
    set job [D 0 show $job_id]
    assert {$job ne {}}
    assert {[llength [dict get $job nodes-delivered]] >= 3}
    assert {[dict get $job state] eq "queued"}

    # We expect at least 3 nodes to have a copy of our job.
    assert {[count_job_copies $job] >= 3}
}

test "ADDJOB, asynchronous replication to multiple nodes" {
    set job_id [D 0 addjob myqueue myjob 5000 replicate 3 async]
    assert {$job_id ne {}}
    set job [D 0 show $job_id]
    assert {$job ne {}}
    assert {[llength [dict get $job nodes-delivered]] >= 3}
    assert {[dict get $job state] eq "queued"}

    # Asynchronous replication does not guarantees the specified number
    # of copies, but here in the test environment no node should fail
    # unless we kill one, so we expect 3 copies.
    wait_for_condition {
        [count_job_copies $job] >= 3
    } else {
        fail "Not enough nodes reached via asynchronous replication"
    }
}

test "Sync ADDJOB fails if not enough nodes are available" {
    catch {D 0 addjob myqueue myjob 5000 replicate 100} job_id
    assert_match {NOREPL*} $job_id
}

test "Sync ADDJOB fails if not enough nodes are reachable" {
    # We kill three instances and send ADDJOB ASAP before the nodes
    # are marked as not reachable.
    kill_instance disque 1
    kill_instance disque 2
    kill_instance disque 3
    set impossible_repl [expr {$::instances_count-3+1}]
    catch {D 0 addjob myqueue myjob 5000 replicate $impossible_repl} job_id
    assert_match {NOREPL*} $job_id
    restart_instance disque 1
    restart_instance disque 2
    restart_instance disque 3
}

# For the probabilistic nature of this test, better to execute it a few times.
for {set j 1} {$j <= 3} {incr j} {
    test "Sync ADDJOB uses more nodes when first contacted are down ($j)" {
        # We kill three instances and send ADDJOB ASAP before the nodes
        # are marked as not reachable.
        kill_instance disque 1
        kill_instance disque 2
        kill_instance disque 3
        # Now let's request a replication equal to the number of nodes still
        # alive. We'll likely also pick a few of the ones that are down.
        # For the replication to succeeed, instance 0 will have to try other
        # nodes before the timeout.
        set max_possible_repl [expr {$::instances_count-3}]
        catch {D 0 addjob myqueue myjob 5000 replicate $max_possible_repl} job_id
        assert_match {DI*} $job_id
        restart_instance disque 1
        restart_instance disque 2
        restart_instance disque 3
    }
    after 1000; # Make likely that restarted nodes fail status is cleared.
}
