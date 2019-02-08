#!/usr/bin/env perl6

use v6.c;
use Test;
use Cache::Memcached;
use CheckSocket;

my $testaddr = "127.0.0.1";
my $port = 11211;

if not check-socket($port, "127.0.0.1") {
    plan 1;
    skip-rest "no memcached server"; 
    exit;

}

my $memd = Cache::Memcached.new(
    servers   => [ "$testaddr:$port" ],
    namespace => "Cache::Memcached::t/$*PID/" ~ (now % 100) ~ "/",
    debug => so %*ENV<MDEBUG>,
);

lives-ok { $memd<test-test-key> = "test value" }, "set as associative";
ok $memd<test-test-key>:exists, "and it exists";
is $memd<test-test-key>, "test value" , "and got the right value back";
is $memd<test-test-key>:delete, "test value" , "delete key and get the right value back";
nok $memd<test-test-key>:exists, "and it no longer exists";

my %data = foo => (^1000).pick, bar => (^1000).pick, baz => (^1000).pick;

for %data.pairs -> $pair {
    $memd{$pair.key} = $pair.value;
    ok $memd{$pair.key}:exists, "added { $pair }";
}

my @keys = %data.keys;

ok so all(@keys) ∈ $memd.keys.list, 'keys returns the keys we expected';

for $memd.keys -> $key {
    ok $memd{$key}:exists, "key $key exists";
}

for $memd.pairs -> $pair (:$key, :$value ) {
    is $memd{$key}, $value, "got expected pair { $pair.perl }";

}

for <foo bar baz> -> $key {
    $memd{$key}:delete;
}

is $memd.keys.elems, 0, "no keys after deleting them";



done-testing();

# vim: expandtab shiftwidth=4 ft=perl6
