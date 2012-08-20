#!/usr/bin/perl

use strict;
use Test::More qw(no_plan);
use FindBin qw($Bin);
use lib ("$Bin/lib", "$Bin/../t/lib");
use RepcachedTest;

my $version     = memcached_version();
my $version_num = version2num($version);
my $support_overflow = $version_num >= version2num('1.2.4') ? 1 : 0;

my($m1, $m2) = new_repcached();
my($sock_m1, $sock_m2) = ($m1->sock, $m2->sock);

for my $sock ([$sock_m1, $sock_m2], [$sock_m2, $sock_m1]) {
    my($sock_m, $sock_b) = ($sock->[0], $sock->[1]);

    print $sock_m "set num 0 0 1\r\n1\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored num");
    sync_get_is($sock_m, $sock_b, "num", 1, "stored 1");

    print $sock_m "incr num 1\r\n";
    is(scalar <$sock_m>, "2\r\n", "+ 1 = 2");
    sync_get_is($sock_m, $sock_b, "num", 2);

    print $sock_m "incr num 8\r\n";
    is(scalar <$sock_m>, "10\r\n", "+ 8 = 10");
    sync_get_is($sock_m, $sock_b, "num", 10);

    print $sock_m "decr num 1\r\n";
    is(scalar <$sock_m>, "9\r\n", "- 1 = 9");
    #sync_get_is($sock_m, $sock_b, "num", '9 ');
    sync_get_is($sock_m, $sock_b, "num", '9');

    print $sock_m "decr num 9\r\n";
    is(scalar <$sock_m>, "0\r\n", "- 9 = 0");
    #sync_get_is($sock_m, $sock_b, "num", '0 ');
    sync_get_is($sock_m, $sock_b, "num", '0');

    print $sock_m "decr num 5\r\n";
    is(scalar <$sock_m>, "0\r\n", "- 5 = 0");
    #sync_get_is($sock_m, $sock_b, "num", '0 ');
    sync_get_is($sock_m, $sock_b, "num", '0');

    printf $sock_m "set num 0 0 10\r\n4294967296\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored 2**32");

    if ($support_overflow) {
        print $sock_m "incr num 1\r\n";
        is(scalar <$sock_m>, "4294967297\r\n", "4294967296 + 1 = 4294967297");
        sync_get_is($sock_m, $sock_b, "num", 4294967297);

        printf $sock_m "set num 0 0 %d\r\n18446744073709551615\r\n", length("18446744073709551615");
        is(scalar <$sock_m>, "STORED\r\n", "stored 2**64-1");
        sync_get_is($sock_m, $sock_b, "num", '18446744073709551615');

        print $sock_m "incr num 1\r\n";
        is(scalar <$sock_m>, "0\r\n", "(2**64 - 1) + 1 = 0");
        #sync_get_is($sock_m, $sock_b, "num", sprintf('%-20s','0'));
        sync_get_is($sock_m, $sock_b, "num", '0');
    }

    print $sock_m "decr bogus 5\r\n";
    is(scalar <$sock_m>, "NOT_FOUND\r\n", "can't decr bogus key");

    print $sock_m "decr incr 5\r\n";
    is(scalar <$sock_m>, "NOT_FOUND\r\n", "can't incr bogus key");

    print $sock_m "set text 0 0 2\r\nhi\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored text");
    print $sock_m "incr text 1\r\n";
    like(scalar <$sock_m>, qr/CLIENT_ERROR/, "hi - 1 = error");
}
