# Check the basic monitoring and failover capabilities.

source "../tests/includes/init-tests.tcl"

test "Cluster should start ok" {
    assert_cluster_state ok
}

test "Killing two nodes" {
    kill_instance redis 5
    kill_instance redis 6
}

test "Nodes should be flagged with the FAIL flag"
}

test "Restarting two nodes" {
    restart_instance redis 5
    restart_instance redis 6
}

test "Nodes FAIL flag should be cleared" {
}
