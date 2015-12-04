source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "LEAVING state can be queried and set" {
    assert {[D 0 cluster leaving] eq {no}}
}

test "LEAVING state can be set" {
    D 0 cluster leaving yes
    assert {[D 0 cluster leaving] eq {yes}}
}

test "LEAVING state progagates to other nodes" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 1 leaving] > 0
    } else {
        fail "Leaving state of node 0 does not propagate to node 1"
    }
}

test "Node 0 is initially empty" {
    assert {[DI 0 registered_jobs] == 0}
}

test "ADDJOB causes external replication" {
    set id [D 0 ADDJOB myqueue myjob 0 REPLICATE 3]
    assert {$id ne {}}
    assert {[DI 0 registered_jobs] == 0}

    foreach_disque_id j {
        set job [D $j SHOW $id]
        if {$job ne {}} break
    }
    assert {[count_job_copies $job] >= 3}
}

test "GETJOB returns a -LEAVING instead of blocking" {
    catch {D 0 GETJOB FROM myqueue} e
    assert_match {LEAVING*} $e
}

test "GETJOB returns previously queued jobs if any" {
    D 0 CLUSTER LEAVING no
    set id [D 0 ADDJOB myqueue myjob 0 REPLICATE 1 RETRY 0]
    D 0 CLUSTER LEAVING yes
    assert {[llength [D 0 GETJOB FROM myqueue]] > 0}
    # The second attempt should fail with LEAVING.
    catch {D 0 GETJOB FROM myqueue} e
    assert_match {LEAVING*} $e
}

test "HELLO shows bad priority for nodes leaving" {
    set myself [get_myself 0]
    set myid [dict get $myself id]
    set hello [D 1 HELLO]
    set pri 0
    foreach node $hello {
        if {[lindex $node 0] eq $myid} {
            set pri [lindex $node 3]
        }
    }
    assert {$pri == 100}
}
