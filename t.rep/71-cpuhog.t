#!/usr/bin/perl

use strict;
use warnings;

use Test::More qw(no_plan);
use FindBin qw($Bin);
use lib ("$Bin/lib", "$Bin/../t/lib");
use RepcachedTest;

my($m1, $m2) = new_repcached();
my($sock_m1, $sock_m2) = ($m1->sock, $m2->sock);

# emulate transient network problems by stopping processes
# stop second process, fill buffers on first one
# XXX use (pid + 1) to bypass timedrun, unsafe
kill 'STOP', $m2->{pid} + 1;

my ($uniq, $key, $val, $vallen);

for my $i (1..1000) {
    $uniq = int(rand()*100000);
    $key = 'settest_1_' . $i;
    $val = 'setval' . ('0' x 1024) . $uniq;
    $vallen = length $val;
    print $sock_m1 "set $key 0 0 $vallen\r\n$val\r\n";
    is(scalar <$sock_m1>, "STORED\r\n", "stored");
}

# stop first process, fill buffers on second one
kill 'STOP', $m1->{pid} + 1;
kill 'CONT', $m2->{pid} + 1;

for my $i (1..1000) {
    $uniq = int(rand()*100000);
    $key = 'settest_2_' . $i;
    $val = 'setval' . ('0' x 1024) . $uniq;
    $vallen = length $val;
    print $sock_m2 "set $key 0 0 $vallen\r\n$val\r\n";
    is(scalar <$sock_m2>, "STORED\r\n", "stored");
}

# release first process
kill 'CONT', $m1->{pid} + 1;

# wait for a while, as normal sync time may not be enough
sleep(1);

mem_get_is($sock_m1, $key, $val, "get $key from 1st");
mem_get_is($sock_m2, $key, $val, "get $key from 2nd");
