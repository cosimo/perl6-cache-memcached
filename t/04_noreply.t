#!/usr/bin/env perl -w

use strict;
use Test;
use Cache::Memcached;
#use IO::Socket::INET;

my $testaddr = "127.0.0.1";
my $port = 11211;
my $msock = IO::Socket::INET.new(host => $testaddr, port => $port);
if ($msock) {
    plan tests => 7;
} else {
    plan skip_all => "No memcached instance running at $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached.new(
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$*PID/" ~ (now % 100) ~ "/",
);

isa_ok($memd, 'Cache::Memcached');


constant count = 30;

$memd.flush_all;

$memd.add("key", "add");
is($memd.get("key"), "add");

for ^count -> $i {
    $memd.set("key", $i);
}
is($memd.get("key"), count - 1);

$memd.replace("key", count);
is($memd.get("key"), count);

for ^count -> $i {
    $memd.incr("key", 2);
}
is($memd.get("key"), count + 2 * count);

for ^count -> $i {
    $memd.decr("key", 1);
}
is($memd.get("key"), count + 1 * count);

$memd.delete("key");
is($memd.get("key"), Nil);

done();
