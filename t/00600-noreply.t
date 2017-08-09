#!/usr/bin/env perl6
#

use v6;

use Test;
use Cache::Memcached;
use CheckSocket;

plan 11;

my $testaddr = "127.0.0.1:11211";
my $port = 11211;

if not check-socket($port, "127.0.0.1") {
    skip-rest "no memcached server";
    exit;

}

my $memd = Cache::Memcached.new(
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$*PID/" ~ (now % 100) ~ "/",
);

isa-ok($memd, 'Cache::Memcached');


constant count = 30;

$memd.flush-all;

$memd.add("key", "add");
is($memd.get("key"), "add", "added a value");

for ^count -> $i {
    $memd.set("key", $i);
}
is($memd.get("key"), count - 1, "value should be " ~ count - 1);

$memd.replace("key", count);
is($memd.get("key"), count, "value should now be " ~ count);

for ^count -> $i {
    $memd.incr("key", 2);
}
is($memd.get("key"), count + 2 * count, "value should now be " ~ count + 2 * count);

for ^count -> $i {
    $memd.decr("key", 1);
}
is(my $count = $memd.get("key"), count + 1 * count, "got the correct decremented value");

lives-ok { $memd.incr("key") }, "incr with a default value";
is $memd.get("key"), $count + 1, "and it was incremented";

lives-ok { $memd.decr("key") }, "decr with a default value";
is $memd.get("key"), $count, "and got the expected value back";

$memd.delete("key");
is($memd.get("key"), Nil, "key is deleted");

done-testing();
