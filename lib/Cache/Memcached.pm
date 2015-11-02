
use v6;

use String::CRC32;

unit class Cache::Memcached:auth<cosimo>:ver<0.04>;

has Bool  $!debug = False;
has Bool  $!no_rehash;
has       %!stats;
has Int   $.compress_threshold is rw;
has Bool  $.compress_enable is rw;
has Bool  $.readonly is rw;
has       &.stat_callback is rw;
has       $.select_timeout is rw;
has Str   $.namespace = "";
has Int   $!namespace_len = 0;
has       @!servers = ();
has       $!active;
has Str   @.buckets = (); # is rw;
has Str   $!pref_ip;
has Int   $!bucketcount = 0;
has       $!_single_sock = False;
has       $!_stime;
has Rat   $!connect_timeout;
has       &.cb_connect_fail;
#as Str   $.parser_class is rw = 'Cache::Memcached::GetParser';
has       @!buck2sock;
has Version $!server-version;

submethod BUILD(:@!servers, Bool :$!debug = False, Str :$namespace) {

    $!namespace = ( $namespace // "" );
    # TODO understand why @!servers is empty here
    if ! @!servers {
        self.log_debug("setting default servers");
        @!servers = "127.0.0.1:11211";
    }

    self.log_debug("Setting servers: ", @!servers);
    self.set_servers(@!servers);
}

# Flag definitions
method F_STORABLE () { return 1 }
method F_COMPRESS () { return 2 }

# Size savings required before saving compressed value
method COMPRESS_SAVINGS () { return 0.20 }  # percent

our $VERSION       = '0.03';
our $HAVE_ZLIB     = 0;
our $HAVE_SOCKET6  = 0;
our $HAVE_XS       = 0;
our $FLAG_NOSIGNAL = 0;


our $SOCK_TIMEOUT = 2.6; # default timeout in seconds

my %host_dead;   # host -> unixtime marked dead until
my %cache_sock;  # host -> socket
my $PROTO_TCP;


method set_pref_ip ($ip) {
    $!pref_ip = $ip
}


method set_servers (@servers) {

    @!servers = @servers;
    $!active = +@servers;

    @!buckets = ();
    $!bucketcount = 0;
    $.init_buckets();
    @!buck2sock = ();
    $!_single_sock = Mu;

    if +@servers == 1 {
        $!_single_sock = @servers[0];
    }
}


method set_cb_connect_fail (&callback) {
    &!cb_connect_fail = &callback;
}


method set_connect_timeout ($timeout) {
    $!connect_timeout = $timeout;
}


method set_debug (Bool $debug = False) {
    $!debug = $debug;
}


method set_readonly (Bool $ro = False) {
    $!readonly = $ro;
}


method set_norehash (Bool $no_rehash = False) {
    $!no_rehash = $no_rehash;
}


method set_compress_threshold (Num $comp_thr) {
    $!compress_threshold = $comp_thr;
}


method enable_compress (Bool $comp = True) {
    $!compress_enable = $comp;
}


method forget_dead_hosts () {
    %host_dead = ();
    @!buck2sock = ();
}


method set_stat_callback (&callback) {
    &!stat_callback = &callback;
}

my %sock_map;  # stringified-$sock -> "$ip:$port"


method _dead_sock ($sock, $ret, $dead_for) {
    if $sock.defined {
        if my $ipport = %sock_map{$sock} {
            %host_dead{$ipport} = now + $dead_for if $dead_for;
            %cache_sock.delete($ipport);
            %sock_map.delete($sock);
        }
    }
    @!buck2sock = ();
    return $ret;
}


method _close_sock ($sock) {
    if my $ipport = %sock_map{$sock} {
        $sock.close();
        %cache_sock.delete($ipport);
        %sock_map.delete($sock);
    }
    @!buck2sock = ();
}


sub _connect_sock ($sock, $sin, $timeout = 0.25) {

    # make the socket non-blocking from now on,
    # except if someone wants 0 timeout, meaning
    # a blocking connect, but even then turn it
    # non-blocking at the end of this function
    
    #my $ret = connect($sock, $sin);

    # TODO FIXME
    my $host = $sock;
    my $port = $sin;

    my $ret;

    try {
        my $sock_obj = IO::Socket::INET.new(host => $host, port => $port);

        if $sock {
            $ret = $sock_obj;
        }
        CATCH {
           default {
              say $_.message;
           }
        }
    }

    return $ret;
}


# Why is this public? I wouldn't have to worry about undef $self if it weren't.
method sock_to_host (Str $host) {

    $.log_debug("sock_to_host");
    if %cache_sock{$host} {
        $.log_debug("cache_sock hit");
        return %cache_sock{$host};
    }
    
    my $now = time;
    my $ip;
    my $port;

    if $host ~~ m/ (.*) \: (\d+) / {
        $ip = $0.Str;
        $port = $1.Int;
        # Get rid of optional IPv6 brackets
        $ip ~~ s:g [ \[ | \] ] = '' if $ip.defined;
    }

    if %host_dead{$host} && %host_dead{$host} > $now {
        return;
    }

    my $timeout = $!connect_timeout //= 0.25;
    my $sock = _connect_sock($ip, $port, $timeout);

    if ! $sock {
        $.log_debug("sock not defined");
        # TODO connect fail callback
        #my &cb = &!cb_connect_fail;
        #if &cb { &cb->() }
        return self._dead_sock($sock, Nil, 20 + 10.rand.Int);
    }

    %sock_map{$sock} = $host;
    %cache_sock{$host} = $sock;

    return $sock;
}


method get_sock ($key) {

    if $!_single_sock {
        return $.sock_to_host($!_single_sock);
    }

    return unless $!active;

    # TODO $key array
    my $hv = _hashfunc($key);
    my $tries = 0;

    while $tries++ < 20 {
        my $host = @!buckets[ $hv % $!bucketcount ];
        my $sock = $.sock_to_host($host);
        return $sock if $sock;
        return if $!no_rehash;
        $hv += _hashfunc($tries ~ $key); # stupid, but works
    }

    return;
}


method init_buckets () {

    $.log_debug("init_buckets with ", @!buckets);

    if not @!buckets.elems {
        $.log_debug("setting buckets");

        for @!servers -> $v {
            $.log_debug("adding server to buckets $v");
            # TODO support weighted servers
            # [ ['127.0.0.1:11211', 2],
            #   ['127.0.0.1:11212', 1], ]
            @!buckets.push($v);
        }

    }
    else {
        say "already got buckets : ", @!buckets;
    }
    $!bucketcount = +@!buckets;

    return $!bucketcount;
}


method disconnect_all () {
    for %cache_sock.values -> $sock {
        $sock.close() if $sock;
    }
    %cache_sock = ();
    @!buck2sock = ();
}


# writes a line, then reads result.  by default stops reading after a
# single line, but caller can override the $check_complete subref,
# which gets passed a scalarref of buffer read thus far.
method _write_and_read (IO::Socket $sock, Str $command, Mu $check_complete?) {

    my $res;
    my $ret = Mu; 
    my $offset = 0;
    my $line = $command;

    #$check_complete //= sub ($ret) {
    #    return ($ret.rindex("\x0D\x0A") + 2) == $ret.chars;
    #};

    # state: 0 - writing, 1 - reading, 2 - done
    my $state = 0;
    my $copy_state = -1;
  
    loop {

        if $copy_state != $state {
            last if $state == 2;
            $copy_state = $state;
        }

        my $to_send = $line.chars;

        $.log_debug("Chars to send: $to_send");


        if $to_send > 0 {
            my $sent = $sock.print($line);
            if $sent == 0 {
                self._close_sock($sock);
                return;
            }
            $to_send -= $sent;
            if $to_send == 0 {
                $state = 1;
            }
            else {
                $line = $line.substr($sent);
            }
        }

        $.log_debug("Receiving from socket");

        $ret = $sock.recv();
        #$ret = "";
        #while (my $c = $sock.recv(1)) {
        #    $ret ~= $c;
        #}

        $.log_debug("Got from socket (recv=" ~ $ret.perl ~ ")");

        if $ret ~~ m/\r\n$/ {
            $.log_debug("Got a terminator (\\r\\n)");
            $state = 2;
        }

    }

    # Improperly finished
    unless $state == 2 {
        self._dead_sock($sock);
        return;
    }

    return $ret;
}


method delete ($key, $time = "") {

    return 0 if ! $!active || $!readonly;

    my $stime;
    my $etime;

    $stime = now if &!stat_callback;

    my $sock = $.get_sock($key);
    return 0 unless $sock;

    %!stats<delete>++;

    # TODO support array keys
    my $cmd = "delete " ~ $!namespace ~ $key ~ $time ~ "\r\n";
    my $res = $._write_and_read($sock, $cmd);

    if &!stat_callback {
        my $etime = now;
        &!stat_callback.($stime, $etime, $sock, 'delete');        
    }

    return $res.defined && $res eq "DELETED\r\n";
}


method add ($key, $value) {
    $._set('add', $key, $value);
}

method replace ($key, $value) {
    $._set('replace', $key, $value);
}

method set ($key, $value) {
    $._set('set', $key, $value);
}

method append ($key, $value) {
    $._set('append', $key, $value);
}

method prepend ($key, $value) {
    $._set('prepend', $key, $value);
}

method _set ($cmdname, $key, $val, Int $exptime = 0) {
    return 0 if ! $!active || $!readonly;
    my $stime;
    my $etime;

    $stime = now if &!stat_callback;
    my $sock = $.get_sock($key);
    return 0 unless $sock;

    my $app_or_prep = ($cmdname eq 'append' or $cmdname eq 'prepend') ?? 1 !! 0;
    %!stats{$cmdname}++;

    my $flags = 0;
    my $len = $val.chars;

    # TODO COMPRESS THRESHOLD support
    #$exptime //= 0;
    #$exptime = $exptime.Int;
    my $line = "$cmdname " ~ $!namespace ~ "$key $flags $exptime $len\r\n$val\r\n";
    my $res  = $._write_and_read($sock, $line);

    if $!debug && $line {
        $line.chop.chop;
        warn "Cache::Memcache: {$cmdname} {$!namespace}{$key} = {$val} ({$line})\n";
    }

    if &!stat_callback {
        my $etime = Time::HiRes::time();
        &!stat_callback.($stime, $etime, $sock, $cmdname);
    }
    
    return $res.defined && $res eq "STORED\r\n";

}

method incr ($key, $offset) {
    $._incrdecr("incr", $key, $offset);
}

method decr ($key, $offset) {
    $._incrdecr("decr", $key, $offset);
}

method _incrdecr ($cmdname, $key, $value) {
    return if ! $!active || $!readonly;

    my $stime;

    $stime = now if &!stat_callback;
    my $sock = $.get_sock($key);
    return unless $sock;

    %!stats{$cmdname}++;
    $value = 1 unless defined $value;

    my $line = "$cmdname " ~ $!namespace ~ "$key $value\r\n";
    my $res = $._write_and_read($sock, $line);

    if &!stat_callback {
        my $etime = now;
        &!stat_callback.($stime, $etime, $sock, $cmdname);
    }

    return defined $res && $res eq "STORED\r\n";
}


method get ($key) {

    my @res;
    my $hv = _hashfunc($key);
    $.log_debug("get(): hash value '$hv'");

    my $sock = $.get_sock($key);
    if $sock.defined {
        $.log_debug("get(): socket '$sock'");

        my $namespace = $!namespace // "";
        my $full_key = $namespace ~ $key;
        $.log_debug("get(): full key '$full_key'");
   
        my $get_cmd = "get $full_key\r\n";
        $.log_debug("get(): command '$get_cmd'");

        @res = self.run_command($sock, $get_cmd);

        %!stats<get>++;

        say "memcache: got ", @res.perl;
    }
    else {
       $.log_debug("No socket ...");
    }

    return @res[1].defined ?? @res[1] !! Nil;
}

sub _hashfunc(Str $key) {
    my $crc = String::CRC32::crc32($key);
    $crc +>= 16;
    $crc +&= 0x7FFF;
    return $crc;
}


method flush_all () {
    my $success = 1;
    my @hosts = @!buckets;

    for @hosts -> $host {
        my $sock = $.sock_to_host($host);
        my @res = $.run_command($sock, "flush_all\r\n");
        $success = 0 unless @res == 1 && @res[0] eq "OK\r\n";
    }

    return $success;
}



# Returns array of lines, or () on failure.
method run_command ($sock, $cmd) {

    return unless $sock;

    my $ret = "";
    my $line = $cmd;

    while (my $res = self._write_and_read($sock, $line)) {
        $line = "";
        $ret ~= $res;
        $.log_debug("Received [$res] total [$ret]");
        last if $ret ~~ /[ OK | END | ERROR ] \r\n $/;
    }

    $ret .= chop;
    $ret .= chop;

    #$ret.split("\r\n") ==> map { "$_\r\n" } ==> my @lines;
    my @lines = $ret.split(/\r\n/);

    return @lines;
}

method stats(*@types) {

    my %stats_hr = ();

    if $!active {
        if not @types.elems {
            @types = <misc malloc self>;
        }

        # The "self" stat type is special, it only applies to this very
        # object.
        if @types ~~ /^self$/ {
            %stats_hr<self> = %!stats.clone;
        }

        my %misc_keys = <bytes bytes_read bytes_written
            cmd_get cmd_set connection_structures curr_items
            get_hits get_misses
            total_connections total_items>.map({ $_ => 1 });

        # Now handle the other types, passing each type to each host server.
        my @hosts = @!buckets;

        HOST: 
        for @hosts -> $host {
            my $sock = $.sock_to_host($host);
            next HOST unless $sock;
            TYPE: 
            for @types.grep({ $_ !~~ /^self$/ }) -> $typename {
                my $type = $typename eq 'misc' ?? "" !! " $typename";
                my $lines = $._write_and_read($sock, "stats$type\r\n", -> $bref {
                    return $bref ~~ /:m^[END|ERROR]\r?\n/;
                });
                unless ($lines) {
                    $._dead_sock($sock);
                    next HOST;
                }

                $lines ~~ s:g/\0//;  # 'stats sizes' starts with NULL?

                # And, most lines end in \r\n but 'stats maps' (as of
                # July 2003 at least) ends in \n. ??
                my @lines = $lines.split(/\r?\n/);

                # Some stats are key-value, some are not.  malloc,
                # sizes, and the empty string are key-value.
                # ("self" was handled separately above.)
                if $typename ~~ any(<malloc sizes misc>) {
                    # This stat is key-value.
                    for @lines -> $line {
                        if $line ~~ /^STAT\s+(\w+)\s(.*)/ {
                            my $key = $0;
                            my $value = $1;
                            if ($key) {
                                %stats_hr<hosts>{$host}{$typename}{$key} = $value;
                            }
                            %stats_hr<total>{$key} += $value
                                if $typename eq 'misc' && $key && %misc_keys{$key};
                            %stats_hr<total>{"malloc_$key"} += $value
                            if $typename eq 'malloc' && $key;
                        }
                    }
                } 
                else {
                    # This stat is not key-value so just pull it
                    # all out in one blob.
                    $lines ~~ s:m/^END\r?\n//;
                    %stats_hr<hosts>{$host}{$typename} ||= "";
                    %stats_hr<hosts>{$host}{$typename} ~= "$lines";
                }
            }
        }
    }

    return %stats_hr;
}

method stats_reset ($types) {
    return 0 unless $!active;

    for @!buckets -> $host {
        my $sock = self.sock_to_host($host);
        next unless $sock;
        my $ok = self._write_and_read($sock, "stats reset");
        unless (defined $ok && $ok eq "RESET\r\n") {
            self._dead_sock($sock);
        }
    }

    return 1;
}

method log_debug(*@message ) {
    if $!debug {
        say @message;
    }
}



=begin pod

=head1 NAME

Cache::Memcached - client library for memcached (memory cache daemon)

=head1 SYNOPSIS

  use Cache::Memcached;

  $memd = new Cache::Memcached {
    'servers' => [ "10.0.0.15:11211", "10.0.0.15:11212", "/var/sock/memcached",
                   "10.0.0.17:11211", [ "10.0.0.17:11211", 3 ] ],
    'debug' => 0,
    'compress_threshold' => 10_000,
  };
  $memd->set_servers($array_ref);
  $memd->set_compress_threshold(10_000);
  $memd->enable_compress(0);

  $memd->set("my_key", "Some value");
  $memd->set("object_key", { 'complex' => [ "object", 2, 4 ]});

  $val = $memd->get("my_key");
  $val = $memd->get("object_key");
  if ($val) { print $val->{'complex'}->[2]; }

  $memd->incr("key");
  $memd->decr("key");
  $memd->incr("key", 2);

=head1 DESCRIPTION

This is the Perl API for memcached, a distributed memory cache daemon.
More information is available at:

  http://www.danga.com/memcached/

=head1 CONSTRUCTOR

=over 4

=item C<new>

Takes one parameter, a hashref of options.  The most important key is
C<servers>, but that can also be set later with the C<set_servers>
method.  The servers must be an arrayref of hosts, each of which is
either a scalar of the form C<10.0.0.10:11211> or an arrayref of the
former and an integer weight value.  (The default weight if
unspecified is 1.)  It's recommended that weight values be kept as low
as possible, as this module currently allocates memory for bucket
distribution proportional to the total host weights.

Use C<compress_threshold> to set a compression threshold, in bytes.
Values larger than this threshold will be compressed by C<set> and
decompressed by C<get>.

Use C<no_rehash> to disable finding a new memcached server when one
goes down.  Your application may or may not need this, depending on
your expirations and key usage.

Use C<readonly> to disable writes to backend memcached servers.  Only
get and get_multi will work.  This is useful in bizarre debug and
profiling cases only.

Use C<namespace> to prefix all keys with the provided namespace value.
That is, if you set namespace to "app1:" and later do a set of "foo"
to "bar", memcached is actually seeing you set "app1:foo" to "bar".

The other useful key is C<debug>, which when set to true will produce
diagnostics on STDERR.

=back

=head1 METHODS

=over 4

=item C<set_servers>

Sets the server list this module distributes key gets and sets between.
The format is an arrayref of identical form as described in the C<new>
constructor.

=item C<set_debug>

Sets the C<debug> flag.  See C<new> constructor for more information.

=item C<set_readonly>

Sets the C<readonly> flag.  See C<new> constructor for more information.

=item C<set_norehash>

Sets the C<no_rehash> flag.  See C<new> constructor for more information.

=item C<set_compress_threshold>

Sets the compression threshold. See C<new> constructor for more information.

=item C<enable_compress>

Temporarily enable or disable compression.  Has no effect if C<compress_threshold>
isn't set, but has an overriding effect if it is.

=item C<get>

my $val = $memd->get($key);

Retrieves a key from the memcache.  Returns the value (automatically
thawed with Storable, if necessary) or undef.

The $key can optionally be an arrayref, with the first element being the
hash value, if you want to avoid making this module calculate a hash
value.  You may prefer, for example, to keep all of a given user's
objects on the same memcache server, so you could use the user's
unique id as the hash value.

=item C<get_multi>

my $hashref = $memd->get_multi(@keys);

Retrieves multiple keys from the memcache doing just one query.
Returns a hashref of key/value pairs that were available.

This method is recommended over regular 'get' as it lowers the number
of total packets flying around your network, reducing total latency,
since your app doesn't have to wait for each round-trip of 'get'
before sending the next one.

=item C<set>

$memd->set($key, $value[, $exptime]);

Unconditionally sets a key to a given value in the memcache.  Returns true
if it was stored successfully.

The $key can optionally be an arrayref, with the first element being the
hash value, as described above.

The $exptime (expiration time) defaults to "never" if unspecified.  If
you want the key to expire in memcached, pass an integer $exptime.  If
value is less than 60*60*24*30 (30 days), time is assumed to be relative
from the present.  If larger, it's considered an absolute Unix time.

=item C<add>

$memd->add($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key doesn't already exist.

=item C<replace>

$memd->replace($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key already exists.  The
opposite of C<add>.

=item C<delete>

$memd->delete($key[, $time]);

Deletes a key.  You may optionally provide an integer time value (in seconds) to
tell the memcached server to block new writes to this key for that many seconds.
(Sometimes useful as a hacky means to prevent races.)  Returns true if key
was found and deleted, and false otherwise.

You may also use the alternate method name B<remove>, so
Cache::Memcached looks like the L<Cache::Cache> API.

=item C<incr>

$memd->incr($key[, $value]);

Sends a command to the server to atomically increment the value for
$key by $value, or by 1 if $value is undefined.  Returns undef if $key
doesn't exist on server, otherwise it returns the new value after
incrementing.  Value should be zero or greater.  Overflow on server
is not checked.  Be aware of values approaching 2**32.  See decr.

=item C<decr>

$memd->decr($key[, $value]);

Like incr, but decrements.  Unlike incr, underflow is checked and new
values are capped at 0.  If server value is 1, a decrement of 2
returns 0, not -1.

=item C<stats>

$memd->stats([$keys]);

Returns a hashref of statistical data regarding the memcache server(s),
the $memd object, or both.  $keys can be an arrayref of keys wanted, a
single key wanted, or absent (in which case the default value is malloc,
sizes, self, and the empty string).  These keys are the values passed
to the 'stats' command issued to the memcached server(s), except for
'self' which is internal to the $memd object.  Allowed values are:

=over 4

=item C<misc>

The stats returned by a 'stats' command:  pid, uptime, version,
bytes, get_hits, etc.

=item C<malloc>

The stats returned by a 'stats malloc':  total_alloc, arena_size, etc.

=item C<sizes>

The stats returned by a 'stats sizes'.

=item C<self>

The stats for the $memd object itself (a copy of $memd->{'stats'}).

=item C<maps>

The stats returned by a 'stats maps'.

=item C<cachedump>

The stats returned by a 'stats cachedump'.

=item C<slabs>

The stats returned by a 'stats slabs'.

=item C<items>

The stats returned by a 'stats items'.

=back

=item C<disconnect_all>

$memd->disconnect_all;

Closes all cached sockets to all memcached servers.  You must do this
if your program forks and the parent has used this module at all.
Otherwise the children will try to use cached sockets and they'll fight
(as children do) and garble the client/server protocol.

=item C<flush_all>

$memd->flush_all;

Runs the memcached "flush_all" command on all configured hosts,
emptying all their caches.  (or rather, invalidating all items
in the caches in an O(1) operation...)  Running stats will still
show the item existing, they're just be non-existent and lazily
destroyed next time you try to detch any of them.

=back

=head1 BUGS

When a server goes down, this module does detect it, and re-hashes the
request to the remaining servers, but the way it does it isn't very
clean.  The result may be that it gives up during its rehashing and
refuses to get/set something it could've, had it been done right.

=head1 COPYRIGHT

This module is Copyright (c) 2003 Brad Fitzpatrick.
All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 FAQ

See the memcached website:
   http://www.danga.com/memcached/

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

Anatoly Vorobey <mellon@pobox.com>

Brad Whitaker <whitaker@danga.com>

Jamie McCarthy <jamie@mccarthy.vg>

=end pod

# vim: ft=perl6 sw=4 ts=4 st=4 sts=4 et
