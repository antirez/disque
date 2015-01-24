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
