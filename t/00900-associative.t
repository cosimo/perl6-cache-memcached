#!/usr/bin/env perl6

use v6;
use Test;
use Cache::Memcached;
use CheckSocket;

plan 5;

my $testaddr = "127.0.0.1";
my $port = 11211;

if not check-socket($port, "127.0.0.1") {
    skip-rest "no memcached server"; 
    exit;

}

my $memd = Cache::Memcached.new(
    servers   => [ "$testaddr:$port" ],
);

lives-ok { $memd<test-test-key> = "test value" }, "set as associative";
ok $memd<test-test-key>:exists, "and it exists";
is $memd<test-test-key>, "test value" , "and got the right value back";
is $memd<test-test-key>:delete, "test value" , "delete key and get the right value back";
nok $memd<test-test-key>:exists, "and it no longer exists";

done-testing();
