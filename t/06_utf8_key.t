#!/usr/bin/env perl -w

use strict;
use Test;
use Cache::Memcached;

my $testaddr = "127.0.0.1";
my $port = 11211;
my $msock = IO::Socket::INET.new(host => $testaddr, port => $port);

if ($msock) {
    plan tests => 2;
} else {
    plan skip_all => "No memcached instance running at $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached.new(
    servers   => [ $testaddr ],
);

my $key = "Ïâ";

ok($memd.set($key, "val1"), "set key1 as val1");
is($memd.get($key), "val1", "get key1 is val1");

done();
