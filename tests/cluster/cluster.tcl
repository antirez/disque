# Cluster-specific test functions.
#
# Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

# Returns a parsed CLUSTER NODES output as a list of dictionaries.
proc get_cluster_nodes id {
    set lines [split [D $id cluster nodes] "\r\n"]
    set nodes {}
    foreach l $lines {
        set l [string trim $l]
        if {$l eq {}} continue
        set args [split $l]
        set node [dict create \
            id [lindex $args 0] \
            addr [lindex $args 1] \
            flags [split [lindex $args 2] ,] \
            ping_sent [lindex $args 3] \
            pong_recv [lindex $args 4] \
            linkstate [lindex $args 5]
        ]
        lappend nodes $node
    }
    return $nodes
}

# Test node for flag.
proc has_flag {node flag} {
    expr {[lsearch -exact [dict get $node flags] $flag] != -1}
}

# Return the number of instances having the specified flag set from the
# POV of the instance 'id'.
proc count_cluster_nodes_with_flag {id flag} {
    set count 0
    foreach node [get_cluster_nodes $id] {
        if {[has_flag $node $flag]} {incr count}
    }
    return $count
}

# Returns the parsed myself node entry as a dictionary.
proc get_myself id {
    set nodes [get_cluster_nodes $id]
    foreach n $nodes {
        if {[has_flag $n myself]} {return $n}
    }
    return {}
}

# Return the value of the specified CLUSTER INFO field.
proc CI {n field} {
    get_info_field [D $n cluster info] $field
}

# Set the cluster node-timeout to all the reachalbe nodes.
proc set_cluster_node_timeout {to} {
    foreach_disque_id id {
        catch {D $id CONFIG SET cluster-node-timeout $to}
    }
}
