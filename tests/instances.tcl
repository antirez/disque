# Multi-instance test framework.
# This is used in order to test Disque and provides
# basic capabilities for spawning and handling N parallel instances.
#
# Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

package require Tcl 8.5

set tcl_precision 17
source ../support/redis.tcl     ; # Disque uses the Redis protocol.
source ../support/util.tcl
source ../support/server.tcl
source ../support/test.tcl

set ::verbose 0
set ::valgrind 0
set ::pause_on_error 0
set ::simulate_error 0
set ::sentinel_instances {}
set ::disque_instances {}
set ::sentinel_base_port 20000
set ::disque_base_port 25000
set ::pids {} ; # We kill everything at exit
set ::dirs {} ; # We remove all the temp dirs at exit
set ::run_matching {} ; # If non empty, only tests matching pattern are run.
set ::condition_max_wait_time 50000 ; # 50 seconds max wait for things to happen
set ::loglevel {}

if {[catch {cd tmp}]} {
    puts "tmp directory not found."
    puts "Please run this test from the Disque source root."
    exit 1
}

# Execute the specified instance of the server specified by 'type', using
# the provided configuration file. Returns the PID of the process.
proc exec_instance {type cfgfile} {
    if {$type eq "disque"} {
        set prgname disque-server
    } else {
        error "Unknown instance type."
    }

    if {$::valgrind} {
        set pid [exec valgrind --track-origins=yes --suppressions=../../../src/valgrind.sup --show-reachable=no --show-possibly-lost=no --leak-check=full ../../../src/${prgname} $cfgfile &]
    } else {
        set pid [exec ../../../src/${prgname} $cfgfile &]
    }
    return $pid
}

# Spawn a disque instance, depending on 'type'.
proc spawn_instance {type base_port count {conf {}}} {
    if {$::loglevel ne {}} {
        lappend conf "loglevel $::loglevel"
    }

    for {set j 0} {$j < $count} {incr j} {
        set port [find_available_port $base_port]
        incr base_port
        puts "Starting $type #$j at port $port"

        # Create a directory for this instance.
        set dirname "${type}_${j}"
        lappend ::dirs $dirname
        catch {exec rm -rf $dirname}
        file mkdir $dirname

        # Write the instance config file.
        set cfgfile [file join $dirname $type.conf]
        set cfg [open $cfgfile w]
        puts $cfg "port $port"
        puts $cfg "dir ./$dirname"
        puts $cfg "logfile log.txt"

        # Add additional config files
        foreach directive $conf {
            puts $cfg $directive
        }
        close $cfg

        # Finally exec it and remember the pid for later cleanup.
        set pid [exec_instance $type $cfgfile]
        lappend ::pids $pid

        # Check availability
        if {[server_is_up 127.0.0.1 $port 100] == 0} {
            abort_sentinel_test "Problems starting $type #$j: ping timeout"
        }

        # Push the instance into the right list
        set link [redis 127.0.0.1 $port]
        $link reconnect 1
        lappend ::${type}_instances [list \
            pid $pid \
            host 127.0.0.1 \
            port $port \
            link $link \
        ]
    }
}

proc cleanup {} {
    puts "Cleaning up..."
    foreach pid $::pids {
        catch {exec kill -9 $pid}
    }
    foreach dir $::dirs {
        catch {exec rm -rf $dir}
    }
}

proc abort_sentinel_test msg {
    puts "WARNING: Aborting the test."
    puts ">>>>>>>> $msg"
    if {$::pause_on_error} pause_on_error
    cleanup
    exit 1
}

proc parse_options {} {
    for {set j 0} {$j < [llength $::argv]} {incr j} {
        set opt [lindex $::argv $j]
        set val [lindex $::argv [expr $j+1]]
        if {$opt eq "--single"} {
            incr j
            set ::run_matching "*${val}*"
        } elseif {$opt eq "--pause-on-error"} {
            set ::pause_on_error 1
        } elseif {$opt eq "--fail"} {
            set ::simulate_error 1
        } elseif {$opt eq {--valgrind}} {
            set ::valgrind 1
        } elseif {$opt eq "--timeout"} {
            incr j
            set ::condition_max_wait_time [expr {$val*1000}]
        } elseif {$opt eq "--loglevel"} {
            incr j
            set ::loglevel $val
            if {[lsearch -exact {warning notice verbose debug} $val] == -1} {
                puts "Unknown loglevel: $val"
                exit 1
            }
        } elseif {$opt eq "--help"} {
            puts "Hello, I'm sentinel.tcl and I run Sentinel unit tests."
            puts "\nOptions:"
            puts "--single <pattern>      Only runs tests specified by pattern."
            puts "--pause-on-error        Pause for manual inspection on error."
            puts "--fail                  Simulate a test failure."
            puts "--valgrind              Run with valgrind."
            puts "--timeout <sec>         Max wait time for tested conditions."
            puts "--help                  Shows this help."
            exit 0
        } else {
            puts "Unknown option $opt"
            exit 1
        }
    }
}

# If --pause-on-error option was passed at startup this function is called
# on error in order to give the developer a chance to understand more about
# the error condition while the instances are still running.
proc pause_on_error {} {
    puts ""
    puts [colorstr yellow "*** Please inspect the error now ***"]
    puts "\nType \"continue\" to resume the test, \"help\" for help screen.\n"
    while 1 {
        puts -nonewline "> "
        flush stdout
        set line [gets stdin]
        set argv [split $line " "]
        set cmd [lindex $argv 0]
        if {$cmd eq {continue}} {
            break
        } elseif {$cmd eq {show-disque-logs}} {
            set count 10
            if {[lindex $argv 1] ne {}} {set count [lindex $argv 1]}
            foreach_disque_id id {
                puts "=== DISQUE $id ===="
                puts [exec tail -$count disque_$id/log.txt]
                puts "---------------------\n"
            }
        } elseif {$cmd eq {ls}} {
            foreach_disque_id id {
                puts -nonewline "Disque $id"
                set errcode [catch {
                    set str {}
                    append str "@[DI $id tcp_port]: "
                    append str "[DI $id role] "
                    if {[DI $id role] eq {slave}} {
                        append str "[DI $id master_host]:[DI $id master_port]"
                    }
                    set str
                } retval]
                if {$errcode} {
                    puts " -- $retval"
                } else {
                    puts $retval
                }
            }
            foreach_sentinel_id id {
                puts -nonewline "Sentinel $id"
                set errcode [catch {
                    set str {}
                    append str "@[SI $id tcp_port]: "
                    append str "[join [S $id sentinel get-master-addr-by-name mymaster]]"
                    set str
                } retval]
                if {$errcode} {
                    puts " -- $retval"
                } else {
                    puts $retval
                }
            }
        } elseif {$cmd eq {help}} {
            puts "ls                     List Disque instances."
            puts "show-disque-logs \[N\] Show latest N lines of logs."
            puts "D <id> cmd ... arg     Call command in Disque <id>."
            puts "DI <id> <field>        Show Disque <id> INFO <field>."
            puts "continue               Resume test."
        } else {
            set errcode [catch {eval $line} retval]
            if {$retval ne {}} {puts "$retval"}
        }
    }
}

# We redefine 'test' as for Sentinel we don't use the server-client
# architecture for the test, everything is sequential.
proc test {descr code} {
    set ts [clock format [clock seconds] -format %H:%M:%S]
    puts -nonewline "$ts> $descr: "
    flush stdout

    if {[catch {set retval [uplevel 1 $code]} error]} {
        if {[string match "assertion:*" $error]} {
            set msg [string range $error 10 end]
            puts [colorstr red $msg]
            if {$::pause_on_error} pause_on_error
            puts "(Jumping to next unit after error)"
            return -code continue
        } else {
            # Re-raise, let handler up the stack take care of this.
            error $error $::errorInfo
        }
    } else {
        puts [colorstr green OK]
    }
}

# Check memory leaks when running on OSX using the "leaks" utility.
proc check_leaks instance_types {
    if {[string match {*Darwin*} [exec uname -a]]} {
        puts -nonewline "Testing for memory leaks..."; flush stdout
        foreach type $instance_types {
            foreach_instance_id [set ::${type}_instances] id {
                if {[instance_is_killed $type $id]} continue
                set pid [get_instance_attrib $type $id pid]
                set output {0 leaks}
                catch {exec leaks $pid} output
                if {[string match {*process does not exist*} $output] ||
                    [string match {*cannot examine*} $output]} {
                    # In a few tests we kill the server process.
                    set output "0 leaks"
                } else {
                    puts -nonewline "$type/$pid "
                    flush stdout
                }
                if {![string match {*0 leaks*} $output]} {
                    puts [colorstr red "=== MEMORY LEAK DETECTED ==="]
                    puts "Instance type $type, ID $id:"
                    puts $output
                    puts "==="
                    incr ::failed
                }
            }
        }
        puts ""
    }
}

# Execute all the units inside the 'tests' directory.
proc run_tests {} {
    set tests [lsort [glob ../tests/*]]
    foreach test $tests {
        if {$::run_matching ne {} && [string match $::run_matching $test] == 0} {
            continue
        }
        if {[file isdirectory $test]} continue
        puts [colorstr yellow "Testing unit: [lindex [file split $test] end]"]
        source $test
        check_leaks {disque}
    }
}

# Command to talk with a Disque instance.
proc D {n args} {
    set r [lindex $::disque_instances $n]
    [dict get $r link] {*}$args
}

proc get_info_field {info field} {
    set fl [string length $field]
    append field :
    foreach line [split $info "\n"] {
        set line [string trim $line "\r\n "]
        if {[string range $line 0 $fl] eq $field} {
            return [string range $line [expr {$fl+1}] end]
        }
    }
    return {}
}

proc DI {n field} {
    get_info_field [D $n info] $field
}

# Iterate over IDs of instances.
proc foreach_instance_id {instances idvar code} {
    upvar 1 $idvar id
    for {set id 0} {$id < [llength $instances]} {incr id} {
        set errcode [catch {uplevel 1 $code} result]
        if {$errcode == 1} {
            error $result $::errorInfo $::errorCode
        } elseif {$errcode == 4} {
            continue
        } elseif {$errcode == 3} {
            break
        } elseif {$errcode != 0} {
            return -code $errcode $result
        }
    }
}

proc foreach_disque_id {idvar code} {
    set errcode [catch {uplevel 1 [list foreach_instance_id $::disque_instances $idvar $code]} result]
    return -code $errcode $result
}

# Get the specific attribute of the specified instance type, id.
proc get_instance_attrib {type id attrib} {
    dict get [lindex [set ::${type}_instances] $id] $attrib
}

# Set the specific attribute of the specified instance type, id.
proc set_instance_attrib {type id attrib newval} {
    set d [lindex [set ::${type}_instances] $id]
    dict set d $attrib $newval
    lset ::${type}_instances $id $d
}

proc get_instance_id_by_port {type port} {
    foreach_${type}_id id {
        if {[get_instance_attrib $type $id port] == $port} {
            return $id
        }
    }
    fail "Instance $type port $port not found."
}

# Kill an instance of the specified type/id with SIGKILL.
# This function will mark the instance PID as -1 to remember that this instance
# is no longer running and will remove its PID from the list of pids that
# we kill at cleanup.
#
# The instance can be restarted with restart-instance.
proc kill_instance {type id} {
    set pid [get_instance_attrib $type $id pid]
    set port [get_instance_attrib $type $id port]

    if {$pid == -1} {
        error "You tried to kill $type $id twice."
    }
    exec kill -9 $pid
    set_instance_attrib $type $id pid -1
    set_instance_attrib $type $id link you_tried_to_talk_with_killed_instance

    # Remove the PID from the list of pids to kill at exit.
    set ::pids [lsearch -all -inline -not -exact $::pids $pid]

    # Wait for the port it was using to be available again, so that's not
    # an issue to start a new server ASAP with the same port.
    set retry 10
    while {[incr retry -1]} {
        set port_is_free [catch {set s [socket 127.0.01 $port]}]
        if {$port_is_free} break
        catch {close $s}
        after 1000
    }
    if {$retry == 0} {
        error "Port $port does not return available after killing instance."
    }
}

# Return true of the instance of the specified type/id is killed.
proc instance_is_killed {type id} {
    set pid [get_instance_attrib $type $id pid]
    expr {$pid == -1}
}

# Restart an instance previously killed by kill_instance
proc restart_instance {type id} {
    set dirname "${type}_${id}"
    set cfgfile [file join $dirname $type.conf]
    set port [get_instance_attrib $type $id port]

    # Execute the instance with its old setup and append the new pid
    # file for cleanup.
    set pid [exec_instance $type $cfgfile]
    set_instance_attrib $type $id pid $pid
    lappend ::pids $pid

    # Check that the instance is running
    if {[server_is_up 127.0.0.1 $port 100] == 0} {
        abort_disque_test "Problems starting $type #$id: ping timeout"
    }

    # Connect with it with a fresh link
    set link [redis 127.0.0.1 $port]
    $link reconnect 1
    set_instance_attrib $type $id link $link
}

