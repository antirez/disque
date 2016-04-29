source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

test "Globalqlen before anything else" {
  set gqlen [D 2 globalqlen qlenqueue]
  assert { $gqlen == 0 }
}

test "Queue jobs into random nodes" {
  for {set j 1} {$j <= 10} {incr j} {
    set target_id [randomInt $::instances_count]
    set body "somejob$j"
    set id [D $target_id addjob qlenqueue $body 5000 replicate 3 retry 60]
    assert {$id ne {}}
  }
}

test "check globalqlen on target node" {
  set gqlen [D 0 globalqlen qlenqueue]
  assert {$gqlen == 10}
}

test "Get jobs from queue" {
  for {set j 1} {$j <= 10} {incr j} {
    set target_id [randomInt $::instances_count]
    set myjob [lindex [D $target_id getjob from qlenqueue] 0]
    assert {[lindex $myjob 0] eq "qlenqueue"}
    assert {[lindex $myjob 1] ne {}}
    assert {[lindex $myjob 2] ne {}}

    set res [D $target_id ackjob [lindex $myjob 1]]
  }
}

test "check globalqlen is now empty" {
  wait_for_condition {
    [D 1 globalqlen qlenqueue] == 0
  } else {
    fail "globalqlen doesn't get empty"
  }
}


