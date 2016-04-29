source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

set extra_instances [ expr {$::instances_count - 1} ]
test "we have extra instances" {
    assert { $extra_instances > 0 }
}

test "check qlen first" {
    set qlen [D 0 qlen randomly_queue]
    assert { $qlen == 0 }
}

test "Add job with queue-randomly" {
    set id [D 0 addjob randomly_queue "somejob" 5000 replicate $extra_instances queue-randomly]
    assert { $id ne {} }
}

test "qlen is still 0" {
    set qlen [D 0 qlen randomly_queue]
    assert { $qlen == 0 }
}

test "the job is some instance" {
    set job [D 1 show $id]
    assert { $job ne {} }
}
