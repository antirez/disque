# Count how many copies of the jobs are found among the nodes that
# may have received a copy.
proc count_job_copies {job {states {queued active}}} {
    set job_id [dict get $job id]
    set delivered [dict get $job nodes-delivered]
    set copies 0
    foreach_disque_id j {
        if {[instance_is_killed disque $j]} continue
        set node_id [dict get [get_myself $j] id]
        if {[lsearch -exact $delivered $node_id] == -1} continue
        set job [D $j show $job_id]
        if {$job ne {} &&
            [lsearch -exact $states [dict get $job state]] != -1} {
             incr copies
        }
    }
    return $copies
}

# Return the list of instance IDs having a given job in the specified state.
#
# If states is an empty string, all the non killed instances not having a copy
# of the job are returned.
proc get_job_instances {job {states {queued}}} {
    set job_id [dict get $job id]
    set res {}
    foreach_disque_id j {
        if {[instance_is_killed disque $j]} continue
        set job [D $j show $job_id]
        if {$job eq {} && $states eq {}} {
            lappend res $j
        } elseif {$job ne {} &&
            [lsearch -exact $states [dict get $job state]] != -1} {
             lappend res $j
        }
    }
    return $res
}
