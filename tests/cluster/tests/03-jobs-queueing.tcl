source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "ADDJOB will initiall only queue the job in the target node" {
    set id [D 0 addjob myqueue myjob 5000 replicate 3]
    set job [D 0 show $id]
    assert {$id ne {}}
    assert {[count_job_copies $job active] == 2}
}
