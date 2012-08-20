#!/usr/bin/perl

use strict;
use Test::More qw(no_plan);
use FindBin qw($Bin);
use lib ("$Bin/lib", "$Bin/../t/lib");
use RepcachedTest;

my($m1, $m2) = new_repcached();
my($sock_m1, $sock_m2) = ($m1->sock, $m2->sock);

for my $sock ([$sock_m1, $sock_m2], [$sock_m2, $sock_m1]) {
    my($sock_m, $sock_b) = ($sock->[0], $sock->[1]);

    print $sock_m "set foo 0 0 6\r\nfooval\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored foo");
    sync_get_is($sock_m, $sock_b, "foo", "fooval");

    print $sock_m "flush_all\r\n";
    is(scalar <$sock_m>, "OK\r\n", "did flush_all");
    sync_get_is($sock_m, $sock_b, "foo", undef);

    # and the other form, specifying a flush_all time...
    my $expire = 4;
    print $sock_m "flush_all $expire\r\n";
    is(scalar <$sock_m>, "OK\r\n", "did flush_all in future");

    print $sock_m "set foo 0 0 4\r\n1234\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored foo = '1234'");
    sync_get_is($sock_m, $sock_b, "foo", '1234');
    sleep $expire+1;
    sync_get_is($sock_m, $sock_b, "foo", undef);
}
