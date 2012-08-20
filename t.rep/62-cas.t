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

    my $key  = 'castest';
    my $val  = 'casval';
    my $val2 = 'casval2';

    # gets foo (should not exist)
    print $sock_m "gets $key\r\n";
    is(scalar <$sock_m>, "END\r\n", "not stored");
    sync_get_is($sock_m, $sock_b, $key, undef);

    # set foo
    print $sock_m "set $key 0 0 ".length($val)."\r\n$val\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "stored");
    sync_get_is($sock_m, $sock_b, $key, $val);

    # cas fail
    print $sock_m "cas $key 0 0 ".length($val2)." 123\r\n$val2\r\n";
    is(scalar <$sock_m>, "EXISTS\r\n", "cas failed for $key");

    # gets foo - success
    my @result;
    @result = mem_gets($sock_m, $key);
    mem_gets_is($sock_m,$result[0],$key,$val);

    @result = mem_gets($sock_b, $key);
    mem_gets_is($sock_b,$result[0],$key,$val);

    # cas success
    print $sock_m "cas $key 0 0 ".length($val2)." $result[0]\r\n$val2\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "cas success, set $key");
    sync_get_is($sock_m, $sock_b, $key, $val2);

    # cas failure (reusing the same key)
    print $sock_m "cas $key 0 0 ".length($val2)." $result[0]\r\n$val2\r\n";
    is(scalar <$sock_m>, "EXISTS\r\n", "reusing a CAS ID");

    # delete foo
    print $sock_m "delete $key\r\n";
    is(scalar <$sock_m>, "DELETED\r\n", "deleted $key");
    sync_get_is($sock_m, $sock_b, $key, undef);

    # cas missing
    print $sock_m "cas $key 0 0 ".length($val2)." $result[0]\r\n$val2\r\n";
    is(scalar <$sock_m>, "NOT_FOUND\r\n", "cas failed, $key does not exist");

    # cas empty
    print $sock_m "cas $key 0 0 ".length($val2)." \r\n$val2\r\n";
    is(scalar <$sock_m>, "ERROR\r\n", "cas empty, throw error");
    # cant parse barval2\r\n
    is(scalar <$sock_m>, "ERROR\r\n", "error out on barval2 parsing");

    # set foo1
    print $sock_m "set foo1 0 0 1\r\n1\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "foo1");
    sync_get_is($sock_m, $sock_b, 'foo1', '1');
    # set foo2
    print $sock_m "set foo2 0 0 1\r\n2\r\n";
    is(scalar <$sock_m>, "STORED\r\n", "foo1");
    sync_get_is($sock_m, $sock_b, 'foo2', '2');

    # gets foo1 check
    print $sock_m "gets foo1\r\n";
    ok(scalar <$sock_m> =~ /VALUE foo1 0 1 (\d+)\r\n/, "gets foo1 regexp success");
    my $foo1_cas_m = $1;
    is(scalar <$sock_m>, "1\r\n","gets foo1 data is 1");
    is(scalar <$sock_m>, "END\r\n","gets foo1 END");

    print $sock_b "gets foo1\r\n";
    ok(scalar <$sock_b> =~ /VALUE foo1 0 1 (\d+)\r\n/, "gets foo1 regexp success");
    my $foo1_cas_b = $1;
    is(scalar <$sock_b>, "1\r\n","gets foo1 data is 1");
    is(scalar <$sock_b>, "END\r\n","gets foo1 END");

    # gets foo2 check
    print $sock_m "gets foo2\r\n";
    ok(scalar <$sock_m> =~ /VALUE foo2 0 1 (\d+)\r\n/,"gets foo2 regexp success");
    my $foo2_cas_m = $1;
    is(scalar <$sock_m>, "2\r\n","gets foo2 data is 2");
    is(scalar <$sock_m>, "END\r\n","gets foo2 END");

    print $sock_b "gets foo2\r\n";
    ok(scalar <$sock_b> =~ /VALUE foo2 0 1 (\d+)\r\n/,"gets foo2 regexp success");
    my $foo2_cas_b = $1;
    is(scalar <$sock_b>, "2\r\n","gets foo2 data is 2");
    is(scalar <$sock_b>, "END\r\n","gets foo2 END");

    # validate foo1 != foo2
    ok($foo1_cas_m =  $foo1_cas_b,"foo1  = foo1 between master and backup");
    ok($foo2_cas_m =  $foo2_cas_b,"foo2  = foo2 between master and backup");
    ok($foo1_cas_m != $foo2_cas_m,"foo1 != foo2 single-gets success");
    ok($foo1_cas_b != $foo2_cas_b,"foo1 != foo2 single-gets success");

    # multi-gets
    print $sock_m "gets foo1 foo2\r\n";
    ok(scalar <$sock_m> =~ /VALUE foo1 0 1 (\d+)\r\n/, "validating first set of data is foo1");
    $foo1_cas_m = $1;
    is(scalar <$sock_m>, "1\r\n",, "validating foo1 set of data is 1");
    ok(scalar <$sock_m> =~ /VALUE foo2 0 1 (\d+)\r\n/, "validating second set of data is foo2");
    $foo2_cas_m = $1;
    is(scalar <$sock_m>, "2\r\n", "validating foo2 set of data is 2");
    is(scalar <$sock_m>, "END\r\n","validating foo1,foo2 gets is over - END");

    # validate foo1 != foo2
    ok($foo1_cas_m != $foo2_cas_m, "foo1 != foo2 multi-gets success");
}
