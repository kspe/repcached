#!/usr/bin/perl

use strict;
use warnings;

use Test::More qw(no_plan);
use FindBin qw($Bin);
use lib ("$Bin/lib", "$Bin/../t/lib");
use RepcachedTest;

my($m1, $m2) = new_repcached();
my($sock_m1, $sock_m2) = ($m1->sock, $m2->sock);

# stop second process, fill buffers on first one
# XXX use (pid + 1) to bypass timedrun, unsafe
kill 'STOP', $m2->{pid} + 1;

my ($uniq, $key, $val, $vallen);

for my $i (1..10) {
    $key = 'settest_1_' . $i;
    $val = 'setval' . $i . ('0' x (1000*1024));
    $vallen = length $val;
    print $sock_m1 "set $key 0 0 $vallen\r\n$val\r\n";
    is(scalar <$sock_m1>, "STORED\r\n", "stored $i");
}

# ask first process to shutdown, and release second one
kill 'INT', $m1->{pid} + 1;
kill 'CONT', $m2->{pid} + 1;

# wait for a while
sleep(2);

for my $i (1..10) {
    $key = 'settest_1_' . $i;
    my $expected = 'setval' . $i . ('0' x (1000*1024)) . "\r\nEND\r\n";
    print $sock_m2 "get $key\r\n";
    my $r = <$sock_m2>;
    $r = <$sock_m2> . <$sock_m2> if $r =~ /VALUE/;
    is(substr($r, 0, 20), substr($expected, 0, 20), "get $i");
    is(length($r), length($expected), "get $i length");
}
