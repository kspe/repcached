package RepcachedTest;
use strict;
use warnings;
use Carp;
use MemcachedTest;
use base qw(MemcachedTest);
use Exporter qw(import);
our @EXPORT = (@MemcachedTest::EXPORT,
               qw(new_repcached sync_get_is
                ));
use Time::HiRes;

our $REP_LAG = 0.8;

sub new_repcached() {
    my ($port_master,$port_backup,$port_rep) = (free_port(),free_port(),free_port());
    return (
        new_memcached("-l 127.0.0.1 -x 127.0.0.1 -X $port_rep -q 65536", $port_master),
        new_memcached("-l 127.0.0.1 -x 127.0.0.1 -X $port_rep -q 65536", $port_backup),
       );
}

sub sync_get_is {
    my($sock_m,$sock_b) = (shift, shift);
    mem_get_is($sock_m, @_);
    Time::HiRes::sleep $REP_LAG;
    mem_get_is($sock_b, @_);
}

1;
