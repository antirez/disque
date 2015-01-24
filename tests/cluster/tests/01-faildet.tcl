source "../tests/includes/init-tests.tcl"

test "Killing two nodes" {
    kill_instance disque 5
    kill_instance disque 6
}

test "Nodes should be flagged with the FAIL flag" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 2
    } else {
        fail "Killed nodes not flagged with FAIL flag after some time"
    }
}

test "Restarting two nodes" {
    restart_instance disque 5
    restart_instance disque 6
}

test "Nodes FAIL flag should be cleared" {
    wait_for_condition {
        [count_cluster_nodes_with_flag 0 fail] == 0
    } else {
        fail "Restarted nodes FAIL flag not cleared after some time"
    }
}
