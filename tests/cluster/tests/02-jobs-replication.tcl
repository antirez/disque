source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "ADDJOB, single node" {
    set id [D 0 addjob myqueue myjob 5000 replicate 1]
    set job [D 0 show $id]
    assert {$id ne {}}
    assert {[llength [dict get $job nodes-delivered]] == 1}
    assert {[dict get $job state] eq "queued"}
}

foreach repl {1 2} {
    test "ADDJOB, single node in OOM, external replication (REPL $repl)" {
        # We want to reach used memory between 75% and 95% full.
        # 3Mb should be enough for all platforms to start with some memory
        # but fill it ASAP.
        D 0 CONFIG SET maxmemory [expr 1024*1024*3]

        # Add jobs until the target node no longer have a copy
        while 1 {
            set id [D 0 addjob myqueue myjob 5000 replicate $repl]
            set job [D 0 show $id]
            if {$job eq {}} break
        }

        # The job should be somewhere even if it is not into the node
        # we added it to.
        set job [list id $id]
        wait_for_condition {
            [llength [get_job_instances $job {queued active}]] >= $repl 
        } else {
            fail "Job was not externally replicated"
        }

        # Restore the original configuration
        D 0 CONFIG SET maxmemory [expr 1024*1024*1024]
    }

    foreach_disque_id id {
        catch {D $id DEBUG FLUSHALL}
    }
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
        assert_match {D-*} $job_id
        restart_instance disque 1
        restart_instance disque 2
        restart_instance disque 3
    }
    after 1000; # Make likely that restarted nodes fail status is cleared.
}

test "Replicating job expires before reaching the replication level" {
    # Put one instance down.
    kill_instance disque 1
    set impossible_repl $::instances_count
    catch {
        D 0 addjob myqueue myjob 15000 replicate $impossible_repl ttl 1
    } job_id
    assert_match {NOREPL*} $job_id
    restart_instance disque 1
}

test "Sync REPLJOB messages are retried against old nodes" {
    # We kill one instance and send ADDJOB ASAP before the nodes
    # are marked as not reachable. However we demand a replication
    # level that cannot be reached while the node is down.
    kill_instance disque 1
    set repl $::instances_count
    D 0 deferred 1
    D 0 addjob myqueue myjob 30000 replicate $repl

    # Wait some time to make sure the node sends REPLJOB to other nodes to start.
    after 2000

    # Restart the instance. If REPLJOB messages are sent again to old nodes
    # the replication should eventually succeed.
    restart_instance disque 1
    D 0 deferred 0
    set job_id [D 0 read]
    assert_match {D-*} $job_id
}

