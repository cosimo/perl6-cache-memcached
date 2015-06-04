#!/usr/bin/env perl -w

use strict;
use Test;
use Cache::Memcached;

my $testaddr = "192.0.2.1:11211";

plan 2;

my $memd = Cache::Memcached.new(
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$*PID/" ~ (now % 100) ~ "/",
);


my $time1 = now;
$memd.set("key", "bar");
my $time2 = now;
# 100ms is faster than the default connect timeout.
ok($time2 - $time1 > .1, "Expected pause while connecting");

# 100ms should be slow enough that dead socket reconnects happen faster than it.
$memd.set("key", "foo");
my $time3 = now;
ok($time3 - $time2 < .1, "Should return fast on retry");

done();
