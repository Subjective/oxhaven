"""Records what Deephaven 42.9's URI types answer for a few hundred inputs (task 13.2a).

Starts the embedded Deephaven 42.9 server (the `deephaven-server` wheel of oracle/requirements.txt)
on a free port, then, in the same Python, the server's console Python, calls the Java parsers
through jpy and writes every answer to `java-42.9.jsonl`, one case per line. The Rust tests of
`crates/oxhaven-uri` read that file.

    python -I tasks/13.2a-uri/probe.py [output]

Needs a venv with `deephaven-server==42.9` and a JDK on PATH. No Java counterpart.
"""

import json
import os
import re
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------------------------

NAMES = [
    "x", "my_table", "MyTable", "t1", "a.b", "a-b", "a*b", "a_b", "_", ".", "..", "*", "-",
    "a~b", "a b", "a/b", "a+b", "a%20b", "a:b", "a@b", "a!b", "a$b", "a&b", "a'b", "a(b", "a,b",
    "a;b", "a=b", "a?b", "a#b", "a[b", "é", "日本", "a\tb", "", "%", "%41",
]

HOSTS = [
    "host", "HOST", "h", "a.b.c", "host.", "my-host", "-host", "host-", "host_name", "1.2.3",
    "1.2.3.4", "999.1.1.1", "127.0.0.1", "[::1]", "::1", "[fe80::1]", "[::1", "[v1.x]",
    "[fe80::1%25eth0]", "[::ffff:1.2.3.4]", "host:80", "host/", "host/s/table", "", "user@host",
    "host?x=1", "host#x", "ho st", "é.com", "xn--e-ufa.com", "a..b", "1host", "host1", "1",
    "a.1", "a.b1", "a.1b", "localhost",
]

PORTS = [None, 0, 1, 42, 80, 443, 10000, 65535, 65536, 99999, 2147483647, -1, -2]

PATHS_LOCAL = [
    "/scope/{n}", "/app/{n}/field/f", "/app/app/field/{n}", "/field/{n}",
]

# Every string here goes through java.net.URI.create and then every Deephaven parser.
RAW = [
    # Local forms.
    "dh:///scope/x", "dh:///scope/my_table", "dh:///app/my_app/field/my_table", "dh:///field/f",
    "dh:/scope/x", "dh:/app/a/field/f", "dh:/field/f", "dh:scope/x", "dh:x", "dh:", "dh",
    "dh:///scope/", "dh:///scope", "dh:///scope//x", "dh:///scope/x/", "dh:///scope/x/y",
    "dh:///scope/a%2Fb", "dh:///scope/a%20b", "dh:///scope/a b", "dh:///scope/%41", "dh:///scope/%4",
    "dh:///scope/%zz", "dh:///scope/%", "dh:///scope/é", "dh:///scope/%C3%A9", "dh:///scope/%FF",
    "dh:///scope/%C3", "dh:///scope/%E2%82", "dh:///scope/%ED%A0%80", "dh:///scope/x?q",
    "dh:///scope/x?", "dh:///scope/x#f", "dh:///scope/x#", "dh:///Scope/x", "dh:///SCOPE/x",
    "dh:///scope/x;p", "dh:///scope/x~", "dh:///scope/a.b-c_d*e", "dh:///scope/a+b",
    "dh:///scope/a:b", "dh:///scope/a@b", "dh:///scope/a$b", "dh:///scope/a,b", "dh:///scope/a=b",
    "dh:///scope/a&b", "dh:///scope/a!b", "dh:///scope/a'b", "dh:///scope/a(b)",
    "dh:///scope/a[b", "dh:///scope/a]b", "dh:///scope/a{b", "dh:///scope/a|b", "dh:///scope/a\\b",
    "dh:///scope/a^b", "dh:///scope/a`b", "dh:///scope/a\"b", "dh:///scope/a<b", "dh:///scope/a>b",
    "dh:///scope/a\u00a0b", "dh:///scope/a\u2028b", "dh:///scope/a\u0085b", "dh:///scope/a\u200bb",
    "dh:///scope/a\u3000b", "dh:///scope/a\u0001b", "dh:///scope/a\u007fb",
    "dh:///app/a/field/b/field/c", "dh:///app//field/f", "dh:///app/a/field/", "dh:///app/a/b/field/c",
    "dh:///app/a/field/b/", "dh:///app/a%2Ffield%2Fb/field/c", "dh:///app/a", "dh:///app/a/field",
    "dh:///field/", "dh:///field/a/b", "dh:///field/a%20b", "dh:////scope/x", "dh:/scope",
    "dh+plain:///scope/x", "dh+plain:///app/a/field/b", "dh+plain:///field/f", "DH:///scope/x",
    "Dh:///field/f", "dh+PLAIN://host/scope/x", "DH://host/scope/x",
    # Remote forms.
    "dh://host/scope/x", "dh://host/app/a/field/b", "dh://host/field/f", "dh+plain://host/scope/x",
    "dh+plain://host/app/a/field/b", "dh+plain://host/field/f", "dh://host:10000/scope/x",
    "dh+plain://host:10000/scope/x", "dh://host:0/scope/x", "dh://host:65535/scope/x",
    "dh://host:65536/scope/x", "dh://host:99999/scope/x", "dh://host:2147483647/scope/x",
    "dh://host:2147483648/scope/x", "dh://host:1111111111111/scope/x", "dh://host:-1/scope/x",
    "dh://host:+1/scope/x", "dh://host:08/scope/x", "dh://host:/scope/x", "dh://host:80:80/scope/x",
    "dh://host:abc/scope/x", "dh://HOST/scope/x", "dh://Host.Example.COM/scope/x",
    "dh://h.example.com./scope/x", "dh://host_name/scope/x", "dh://host_name/field/f",
    "dh://host_name/app/a/field/f", "dh://-host/scope/x", "dh://host-/scope/x",
    "dh://ho-st/scope/x", "dh://1.2.3/scope/x", "dh://1.2.3.4/scope/x", "dh://256.1.1.1/scope/x",
    "dh://999.999.999.999/scope/x", "dh://1.2.3.4.5/scope/x", "dh://1host/scope/x",
    "dh://a.1/scope/x", "dh://a.1b/scope/x", "dh://a.b1/scope/x", "dh://a..b/scope/x",
    "dh://.a/scope/x", "dh://127.0.0.1:10000/scope/x", "dh://[::1]/scope/x", "dh://[::1]:8080/scope/x",
    "dh://[::1]:/scope/x", "dh://[fe80::1%25eth0]/scope/x", "dh://[fe80::1%eth0]/scope/x",
    "dh://[::ffff:1.2.3.4]/scope/x", "dh://[1:2:3:4:5:6:7:8]/scope/x", "dh://[1:2:3:4:5:6:7:8:9]/scope/x",
    "dh://[::1/scope/x", "dh://::1/scope/x", "dh://[]/scope/x", "dh://[v1.x]/scope/x",
    "dh://[gggg::1]/scope/x", "dh://[::1]x/scope/x", "dh://user@host/scope/x",
    "dh://user:pw@host/scope/x", "dh://@host/scope/x", "dh://ho%41st/scope/x", "dh://ho st/scope/x",
    "dh://é.com/scope/x", "dh://xn--e-ufa.com/scope/x", "dh://host/scope/x?y", "dh://host/scope/x#f",
    "dh://host/scope/x?y=1#f", "dh://host/scope/", "dh://host/scope", "dh://host/", "dh://host/x",
    "dh://host/scope/a/b", "dh://host/scope/a%2Fb", "dh://host/scope/a%20b", "dh://host/scope/é",
    "dh://host/app/a/field/b/field/c", "dh://host/field/a/b", "dh://host;x/scope/x",
    "dh://ho$t/scope/x", "dh://h~st/scope/x", "dh://h!st/scope/x", "dh://host:80/", "dh://host//scope/x",
    "dh://host:10000", "dh://h/scope/x/", "dh://localhost/scope/t",
    # Targets.
    "dh://host", "dh://host:42", "dh+plain://host", "dh+plain://host:42", "dh://HOST", "dh://host:",
    "dh://host:0", "dh://host:65536", "dh://host:-1", "dh://host:-2", "dh://host:1111111111111",
    "dh://host:80:80", "host", "dh://host?bad=1", "dh://host#bad", "dh://", "dh:///", "dh://host/",
    "dh://host/s/table", "dh://user@host", "dh://[::1]", "dh://[::1]:5", "dh://[::1", "dh://::1",
    "dh://host:abc", "dh+plain://host:10000", "dh://host_name", "dh://host_name:80", "dh://1.2.3",
    "dh://-x", "dh://a.b.c.", "dh://127.0.0.1", "dh://[fe80::1%25eth0]:10000", "dh://ho%41st",
    "dh://host?", "dh://host#", "dh://host?uri=x", "dh+tls://host", "http://host", "dh+plain:host",
    "//host", "//host/scope/x",
    # Proxied (gateway) forms.
    "dh://gateway?uri=dh://host/app/appId/field/fieldName", "dh://gateway?uri=dh://host/field/fieldName",
    "dh://gateway?uri=dh://host/scope/variable", "dh://gateway?uri=dh://host:31337/scope/variable",
    "dh://gateway:42?uri=dh://host/scope/variable", "dh://gateway:42?uri=dh://host:31337/field/f",
    "dh://gateway-1?uri=dh+plain://gateway-2?uri=dh://host/field/fieldName",
    "dh://g1?uri=dh://g2?uri=dh://g3?uri=dh://host/scope/t",
    "dh://host?uri=some-protocol%3A%2F%2Fuser%40some-host%3A15%2Fsome-path%3FsomeQuery%3Dok%23myfragment",
    "dh://gateway?uri=parquet%3A%2F%2F%2Fdata%2Ftest.parquet", "dh://host?uri=custom%3A%2F%2F%2Ffoo%3Fbar%3Dbaz",
    "dh://host?uri=dh%3A%2F%2Fuser%40some-host%3A15%2Fsome-path%3FsomeQuery%3Dok%23myfragment",
    "dh://gateway?uri=", "dh://gateway?uri", "dh://gateway?url=dh://host/scope/x", "dh://gateway?URI=x",
    "dh://gateway?uri=x", "dh://gateway?uri=dh:///scope/x", "dh://gateway?uri=dh://host",
    "dh://gateway?uri=parquet:///data/t.parquet", "dh://gateway?uri=custom:///foo",
    "dh://gw?uri=a%20b", "dh://gw?uri=%", "dh://gw?uri=%41", "dh://gw?uri=dh://host/scope/x&foo=1",
    "dh://gw?foo=1&uri=dh://host/scope/x", "dh://gw/?uri=dh://host/scope/x", "dh://gw?uri=dh://host/scope/x#frag",
    "dh://gw?uri=dh://host/scope/x%23frag", "dh://gw?uri=uri=x", "dh://gw?uri==", "dh://gw?uri=:x",
    "dh://gw?uri=dh+plain://host:1/app/a/field/b", "dh+plain://gw:9?uri=dh://h/field/f",
    "dh://gw?uri=dh%3A%2F%2Fhost%2Fscope%2Fx", "dh://gw?uri=csv%2Bhttps%3A%2F%2Fexample.com%2Fa.csv",
    "dh://gw?uri=http%3A%2F%2Fexample.com%2Fa%2520b", "dh://gw?uri=x%3Ay%20z", "dh://gw?uri=é:x",
    "dh://gw?uri=%C3%A9:x", "dh://gw?uri=dh://host_name/scope/x", "dh://gw?uri=dh://host/scope/a%2520b",
    "dh://user@gw?uri=dh://host/scope/x", "dh://gw:80:80?uri=dh://host/scope/x",
    "dh://host_name?uri=dh://host/scope/x", "dh://[::1]:9?uri=dh://[::2]/scope/x",
    "dh://gw?uri=mailto:a@b", "dh://gw?uri=a+b:c", "dh://gw?uri=dh://gw2?uri=parquet%253A%252F%252F%252Fd",
    # Custom URIs.
    "parquet:///data/t.parquet", "parquet:/data/t.parquet", "parquet://host/data", "csv+https://example.com/a.csv",
    "some-protocol://user@some-host:15/some-path?someQuery=ok#myfragment", "mailto:a@b", "file:/x",
    "urn:isbn:123", "x:", "x", "/abs/path", "relative", "", "a.b-c+d://h", "x://h/p?q#f#g",
    "x://h/%E2%82%AC", "x://h/€", "http://example.com/a b", "http://example.com/%zz", "http://[::1",
    "http://a:b:c", ":foo", "1x://a", "+x://a", "x y://a", "x_y://a", "x://h/p?q=a b", "x://h/p#a b",
    "x://h/p#a#b", "x:/p?q", "x:?q", "x:#f", "x:a b", "x://h?q", "x://h#f", "x://", "x:///",
    "x://h:", "x://h:1:2", "x://[::1]:99999999999", "x://u@[::1]:1/p", "x://%41/p", "x://h/p%2", "x:%41",
    "x:\u00e9", "x:/\u00a0", "x:/\u0001", "x:/a\u007fb", "#f", "?q", "//", "///", "/", ".", "..", "./a",
    "a:b:c", "A:b", "a1+-.:b", "x://h/p?%", "x://h/p#%", "x://h/p?%zz", "x://h/[a]", "x://h/p?[a]",
    "x://h/p#[a]", "x://[::1]/p", "x://a@b@c/p", "x://h/\\", "x://h/^", "x://h/`", "x://h/{",
    "x://h/|", "x://h/}", "x://h/\"", "x://h/<", "x://h/>", "x://h /p", "x://h\u00e9/p",
    "x://ü.de/p", "x://[::1%eth0]/p", "x://h/p\u3000q", "x://u\u00e9@h/p", "x://u%zz@h/p",
    "x://u@h:1x/p", "x://[::1]:1x/p", "x://[::1]]/p", "x://h]/p", "x://h[/p",
    # Host names and IPv4 addresses.
    "x://1/p", "x://123/p", "x://1.a/p", "x://1a.b/p", "x://a.b-1/p", "x://a-.b/p", "x://a.-b/p",
    "x://a.b-/p", "x://01.2.3.4/p", "x://001.002.003.004/p", "x://1.2.3.4./p", "x://0.0.0.0/p",
    "x://255.255.255.255/p", "x://255.255.255.256/p", "x://1.2.3.04/p", "x://1.2.3.4a/p", "x://a.b.c.d.e.f/p",
    "x://a-b-c/p", "x://a--b/p", "x://A1-b2.C3/p", "x://a.B/p", "x://a.9/p", "x://9a.b9/p", "x://-/p", "x://./p",
    "x://../p", "x://a./p", "x://1./p", "x://1.2./p", "x://a.1./p",
    # Authorities that only the strict server-based parser can explain.
    "x://a[b/p", "x://h]:1/p", "x://[::1]:1]/p", "x://u@h]/p", "x://[::1]@h/p", "x://h:1[/p", "x://h:[1]/p",
    "x://[[::1]]/p", "x://]/p", "x://[/p", "x://u[@h/p", "x://1.2.3.4]/p", "x://1.2.3.4[x/p", "x://-h[/p",
    "x://a.1[/p", "x://a..b[/p", "x://h_[/p", "x://u@[::1/p", "x://[::1]:/p", "x://[::1]:x/p", "x://@[::1]/p",
    "x://h%41[/p", "x://h:99999999999[/p", "x://[::1]:99999999999/p", "x://h:99999999999/p",
    "x://h\"/p", "x://h{/p", "x://h|/p", "x://h\\/p", "x://h^/p", "x://h`/p", "x://h</p", "x://h>/p",
    "x://h%zz/p", "x://h%/p", "x://h%4/p", "x://?q", "x://#f", "//?q", "//#f", "//h", "///p",
    # IPv6.
    "x://[::]/p", "x://[1::2::3]/p", "x://[:1]/p", "x://[1:]/p", "x://[12345::1]/p", "x://[1:2:3:4:5:6:7::]/p",
    "x://[::1:2:3:4:5:6:7]/p", "x://[1:2:3:4:5:6:7:8:9]/p", "x://[1:2:3:4:5:6:7:8::]/p", "x://[::1.2.3]/p",
    "x://[::1.2.3.4.5]/p", "x://[::256.1.1.1]/p", "x://[1:2:3:4:5:6:1.2.3.4]/p", "x://[1:2:3:4:5:6:7:1.2.3.4]/p",
    "x://[::1.2.3.4:5]/p", "x://[::%]/p", "x://[::1%]/p", "x://[::1%25]/p", "x://[::1%e-0]/p", "x://[::1%e_0]/p",
    "x://[::1%e.0]/p", "x://[::1%e~0]/p", "x://[::1%é]/p", "x://[::1%%]/p", "x://[::1%e:0]/p", "x://[A::B]/p",
    "x://[aBcD::eF01]/p", "x://[0:0:0:0:0:0:0:0]/p", "x://[::ffff:0.0.0.0]/p", "x://[1:2:3:4:5:6:7:8]:1/p",
    "x://[::1::]/p", "x://[:::]/p", "x://[1:::2]/p", "x://[::01.2.3.4]/p", "x://[g::]/p", "x://[::g]/p",
    "x://[1.2.3.4]/p", "x://[::1 ]/p", "x://[0001:0002::]/p", "x://[00001::]/p", "x://[fe80::1%25eth0%25]/p",
    "x://[::1]a:1/p", "x://[]:1/p", "x://[/p]", "x://[::1]#f", "x://[::1]?q",
    # Decoding of escaped octets that are not UTF-8.
    "x:/%ED%A0%80%41", "x:/%FF%FE", "x:/%C3%28", "x:/%E2%28%A1", "x:/%F0%90%80", "x:/%F0%28%8C%BC",
    "x:/%C0%AF", "x:/%F8%88%80%80%80", "x:/%80", "x:/%80%80", "x:/%E0%80%80", "x:/%F4%90%80%80",
    "x:/%F0%9F%98%80", "x:/%f0%9f%98%80", "x:/%C3%A9%C3", "x:/%C3a%A9", "x:/%E2%82%ACa", "x:/%00",
    "x:/%ED%BF%BF", "x:/%EF%BF%BF", "x:/%F4%8F%BF%BF", "x:/%C2", "x:/%C2%C2%A9", "x:/%E2%82%E2%82%AC",
    "x:/%FE", "x:/%C1%BF", "x:/%E0%9F%BF", "x:/%F0%8F%BF%BF", "x:/%F5%80%80%80", "x:/%E2%82%AC%",
    # Indexes count UTF-16 units.
    "x://h/\U0001F600 b", "x://h/\U0001F600\U0001F600|", "\U0001F600:x", "x:\U0001F600 ", "x://h/\U0001F600%zz",
    "x://h/\U0001F600", "x://\U0001F600/p", "x://h/?\U0001F600", "x://h/#\U0001F600",
    "x://h/\U0001F600\u3000",
    # Opaque and scheme edge cases.
    "x:/", "x://h/p?", "x://h/p#", "x:a#", "x:a?b#c", "x:a/b", "x:@", "x:[", "x:a[b", "x:a]", "x:%zz",
    "x:#", "x::", "x:/:", "a", "a:", "a:/", "a+:x", "a-:x", "a.:x", "Z9:x", "x:\u00a0", "x:\u0001",
    "/:x", "?:x", "#:x", "a/b:c", "a?b:c", "a#b:c", "%41:x", "a%41:x", "dh/:x", "dh:/",
    "x:/p;q", "x:/p;q/r;s", "x:/a@b:c&d=e+f$g,h", "x://h/a@b:c&d=e+f$g,h;i", "x://h/~!*'()-._",
    "x://h/?a/b?c:d@e", "x://h/#a/b?c:d@e",
    # More calibration: IPv6 lengths, IPv4 inside IPv6, strict host names, decoding, line ends.
    "x://[1:2:3]/p", "x://[::1:2:3:4:5:6:7:8]/p", "x://[1:2:3:4:5:6:7:8:9::]/p", "x://[1::2:3:4:5:6:7:8]/p",
    "x://[1:2:3:4:5:6:7]/p", "x://[1:2:3:4:5:6::1.2.3.4]/p", "x://[::1.256.1.1]/p", "x://[::1.2.3.]/p",
    "x://[::1..2.3]/p", "x://[::1234.1.1.1]/p", "x://[::0001.1.1.1]/p", "x://[::a.2.3.4]/p", "x://[::1.2.3.4]/p",
    "x://[::1.2.3.4%x]/p", "x://[::1.2.3.4a]/p", "x://[1.2.3.4::]/p", "x://[::1:]/p", "x://[::1:%x]/p",
    "x://[1:2:3:4:5:6:7:8%x]/p", "x://[g::%e-0]/p", "x://[::%e-0]/p", "x://[::1%x]x/p", "x://[::1%x/p",
    "x://[::1]:65536/p", "x://[::1]:2147483647/p", "x://[::1]:2147483648/p", "x://u@[::1]:/p",
    "x://a.1:[/p", "x://1.2.3:[/p", "x://a-:[/p", "x://a-[/p", "x://a-.b[/p", "x://a.b-[/p", "x://.[/p",
    "x://a.:[/p", "x://-[/p", "x://h:[/p", "x://h:1:2[/p", "x://u@u@h[/p", "x://h@[/p", "x://[@h/p",
    "x://h]@x/p", "x://[::1]:1@h/p", "x://h:1x[/p", "x://u:p@[::1]:1/p", "x://u%41@h:1/p",
    "x:/%ED%A0", "x:/%ED%A0%41", "x:/%ED%9F%BF", "x:/%ED%C0", "x:/%ED", "x:/%ED%80%80%ED%A0%80",
    "x:/%F0%9F%98", "x:/%F0%9F", "x:/%F0%9F%98%41", "x:/%E2%41", "x:/%F4%8F", "x:/%C3%A9%A9",
    "dh:///scope/a%0Ab", "dh:///scope/a%0D", "dh:///scope/a%C2%85", "dh:///scope/a%E2%80%A8",
    "dh:///scope/a%E2%80%A9b", "dh:///scope/%0A", "dh:///app/a%0A/field/f", "dh:///field/%0D%0A",
    "dh:///app/a/field/b%0A/field/c", "dh://gw?uri=a%0Ab", "dh://gw?uri=dh://h/scope/x%0A",
    "dh:///scope/a%0B", "dh:///scope/a%0C", "dh:///scope/%00", "dh:///app/a/field/b/field/",
    "dh:///app/a/field//field/b", "dh:///app//field//field/b", "dh:///app/field/field/field/field",
    "dh://h:1/app/a/field/b/field/", "dh:///scope/%2A", "dh:///scope/%2a",
    "x://:80/p", "x://u@/p", "x://u@:80/p", "x://:/p", "x://:[/p", "x://u@:[/p", "x://:1[/p", "x://@/p",
    "x://0001.1.1.1/p", "x://1.2.3.4:80/p", "x://1.2.3.4:/p", "x://1.2.3.4.:[/p", "x://1.2.3.4x:[/p",
    "dh://:80/scope/x", "dh://u@/scope/x", "dh://@host/scope/x",
    "x://h/p?[%41]%41", "x://h/p#[%41]%41", "x:[%41]%41", "x://[::1%25]/p?[%25]", "x://u%41@[::1%25]:1/p",
    "x://h/p?%5B%41%5D", "x://[%41]/p", "x://[::1]%41/p", "x://u%zz@[::1]/p", "x://[::1%zz]/p",
    "x://%zz[/p", "x://[%zz/p", "x://h[%zz/p", "x://h /%zz", "x://h\"%zz/p",
    "dh:///scope/%F0%9F%98%80", "dh://gw?uri=x%3A%2F%F0%9F%98%80",
]


def raw_inputs():
    out = list(RAW)
    for name in NAMES:
        for pattern in PATHS_LOCAL:
            path = pattern.format(n=name)
            out.append("dh://" + path)
            out.append("dh://host" + path)
    seen = set()
    unique = []
    for s in out:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique


CUSTOM_FOR_REMOTE = [
    "parquet:///data/t.parquet", "custom:///foo?bar=baz",
    "some-protocol://user@some-host:15/some-path?someQuery=ok#myfragment", "x://h/€", "x:é",
    "x://h/a%20b", "x://h/a+b", "x://h/a*b-c.d_e~f", "mailto:a@b", "x://[::1]:1/p", "urn:isbn:1",
    "x://h/p?a=b&c=d", "x:/\u00a0", "x://h/%E2%82%AC", "x://h/\U0001F600",
]

EQUAL_PAIRS = [
    ("x://h/a", "x://h/a"), ("x://H/a", "x://h/a"), ("X://h/a", "x://h/a"), ("x://h/A", "x://h/a"),
    ("x://h/%2f", "x://h/%2F"), ("x://h/%41", "x://h/A"), ("x://h:1/a", "x://h/a"), ("x://u@h/a", "x://U@h/a"),
    ("x://h/a?q", "x://h/a?Q"), ("x://h/a#f", "x://h/a"), ("x://h/a#%7e", "x://h/a#%7E"),
    ("x:abc", "x:abc"), ("x:abc", "X:abc"), ("x:ab%2fc", "x:ab%2Fc"), ("x:abc", "x:ABC"),
    ("x://h_1/a", "x://H_1/a"), ("x://[::1]/a", "x://[::1]/a"), ("x://[::A]/a", "x://[::a]/a"),
    ("x://h/a", "x://h/a/"), ("x://h", "x://h/"),
    ("dh://host/scope/x", "dh://host/scope/x"), ("dh://HOST/scope/x", "dh://host/scope/x"),
    ("dh://host:10000/scope/x", "dh://host/scope/x"), ("dh+plain://host/scope/x", "dh://host/scope/x"),
    ("dh://gw?uri=x://H/a", "dh://gw?uri=x://h/a"), ("dh://gw?uri=x://h/%2f", "dh://gw?uri=x://h/%2F"),
    ("dh://gw?uri=x://h/a", "dh://gw?uri=x%3A%2F%2Fh%2Fa"), ("dh://gw?uri=X://h/a", "dh://gw?uri=x://h/a"),
    ("dh://g?uri=dh://h/field/f", "dh://g?uri=dh://h/field/f"),
]


def fix_garbled(case, s):
    """Java messages reach Python through modified UTF-8, which jpy reads as UTF-8: a character
    outside the BMP comes back as six U+FFFD, a NUL as two. Put the input's character back
    (inputs have at most one such character)."""
    astral = {ch for ch in s if ord(ch) >= 0x10000} | ({"\U0001F600"} if "%F0%9F%98%80" in s else set())
    if "%00" in s:
        garbled, ch = "\ufffd" * 2, "\x00"
    elif len(astral) == 1:
        garbled, ch = "\ufffd" * 6, next(iter(astral))
    else:
        return case

    def fix(value):
        if isinstance(value, str):
            return value.replace(garbled, ch)
        if isinstance(value, dict):
            return {k: fix(v) for k, v in value.items()}
        return value

    return {k: (fix(v) if k != "input" else v) for k, v in case.items()}


def join_surrogates(value):
    """jpy hands a Java string over as UTF-16 units; join each surrogate pair into one character."""
    if isinstance(value, str):
        return value.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
    if isinstance(value, dict):
        return {k: join_surrogates(v) for k, v in value.items()}
    if isinstance(value, list):
        return [join_surrogates(v) for v in value]
    return value


# ---------------------------------------------------------------------------------------------
# Calling Java
# ---------------------------------------------------------------------------------------------


def main() -> int:
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "java-42.9.jsonl")
    from deephaven_server import Server

    server = Server(
        host="127.0.0.1",
        port=free_port(),
        jvm_args=["-Xmx512m", "-XX:ActiveProcessorCount=2", "-Dauthentication.psk=uri-probe"],
    )
    server.start()

    import jpy

    URI = jpy.get_type("java.net.URI")
    DeephavenTarget = jpy.get_type("io.deephaven.uri.DeephavenTarget")
    QueryScopeUri = jpy.get_type("io.deephaven.uri.QueryScopeUri")
    ApplicationUri = jpy.get_type("io.deephaven.uri.ApplicationUri")
    FieldUri = jpy.get_type("io.deephaven.uri.FieldUri")
    RemoteUri = jpy.get_type("io.deephaven.uri.RemoteUri")
    CustomUri = jpy.get_type("io.deephaven.uri.CustomUri")
    UriHelper = jpy.get_type("io.deephaven.uri.UriHelper")

    JString = jpy.get_type("java.lang.String")
    JClass = jpy.get_type("java.lang.Class")
    reflect_create = JClass.forName("java.net.URI").getMethod(
        "create", jpy.array("java.lang.Class", [JClass.forName("java.lang.String")])
    )

    def call(fn):
        try:
            return {"ok": fn()}
        except Exception as e:  # jpy raises RuntimeError("<class>: <message>\n\tat ...")
            text = str(e)
            if text.startswith("java.lang.reflect.InvocationTargetException"):
                text = text.split("\ncaused by ", 1)[1]
            text = text.split("\n\tat ")[0].split("\ncaused by ")[0]
            cls, _, msg = text.partition(": ")
            if not _:
                cls, msg = text, None
            return {"err": {"class": cls, "message": msg}}

    def create(s):
        """URI.create(s). jpy cuts a character outside the BMP to its low 16 bits when it hands a
        str to Java, so such inputs go in as a String made from UTF-8 bytes, through reflection."""
        if all(ord(ch) < 0x10000 for ch in s):
            return URI.create(s)
        data = jpy.array("byte", [b - 256 if b > 127 else b for b in s.encode("utf-8")])
        try:
            return reflect_create.invoke(None, jpy.array("java.lang.Object", [JString(data, "UTF-8")]))
        except Exception as e:
            # The message comes back with the input garbled (jpy again); keep Java's reason and
            # index, and put the input back.
            text = str(e).split("\ncaused by ", 1)[1].split("\n\tat ")[0]
            head = re.match(r"^(.*? at index \d+): ", text).group(1)
            raise RuntimeError(f"{head}: {s}") from None

    def target_desc(t):
        return {
            "secure": bool(t.isSecure()),
            "host": t.host(),
            "port": t.port().getAsInt() if t.port().isPresent() else None,
            "string": t.toString(),
        }

    def desc(u):
        name = u.getClass().getSimpleName()
        # jpy returns an object as its declared type; cast it to its class to see its fields.
        u = jpy.cast(u, jpy.get_type(u.getClass().getName()))
        if name == "ImmutableQueryScopeUri":
            d = {"kind": "scope", "name": u.variableName()}
        elif name == "ImmutableApplicationUri":
            d = {"kind": "app", "app": u.applicationId(), "field": u.fieldName()}
        elif name == "ImmutableFieldUri":
            d = {"kind": "field", "name": u.fieldName()}
        elif name == "ImmutableRemoteUri":
            d = {"kind": "remote", "target": target_desc(u.target()), "inner": desc(u.uri())}
        elif name == "ImmutableCustomUri":
            d = {"kind": "custom", "uri": u.uri().toString()}
        else:
            raise AssertionError(name)
        d["string"] = u.toString()
        return d

    def components(u):
        return {
            "string": u.toString(),
            "scheme": u.getScheme(),
            "opaque": bool(u.isOpaque()),
            "raw_ssp": u.getRawSchemeSpecificPart(),
            "ssp": u.getSchemeSpecificPart(),
            "raw_authority": u.getRawAuthority(),
            "authority": u.getAuthority(),
            "raw_user_info": u.getRawUserInfo(),
            "user_info": u.getUserInfo(),
            "host": u.getHost(),
            "port": u.getPort(),
            "raw_path": u.getRawPath(),
            "path": u.getPath(),
            "raw_query": u.getRawQuery(),
            "query": u.getQuery(),
            "raw_fragment": u.getRawFragment(),
            "fragment": u.getFragment(),
            "ascii": u.toASCIIString(),
        }

    def roundtrip(parse, value):
        def go():
            again = parse(create(join_surrogates(value.toString())))
            return bool(again.equals(value))

        return call(go)

    cases = []
    for s in raw_inputs():
        case = {"section": "parse", "input": s}
        parsed = call(lambda: create(s))
        if "err" in parsed:
            case["uri"] = parsed
        else:
            u = parsed["ok"]
            case["uri"] = {"ok": components(u)}
            for key, cls, describe in [
                ("target", DeephavenTarget, target_desc),
                ("scope", QueryScopeUri, desc),
                ("app", ApplicationUri, desc),
                ("field", FieldUri, desc),
                ("remote", RemoteUri, desc),
            ]:
                case[key + "_well_formed"] = bool(cls.isWellFormed(u))
                r = call(lambda: cls.of(u))
                if "ok" in r:
                    value = r["ok"]
                    case[key] = {"ok": describe(value)}
                    case[key + "_roundtrip"] = roundtrip(cls.of, value)
                else:
                    case[key] = r
            r = call(lambda: CustomUri.of(u))
            case["custom"] = {"ok": desc(r["ok"])} if "ok" in r else r
            case["is_local_path"] = bool(UriHelper.isLocalPath(u))
            case["is_remote_path"] = bool(UriHelper.isRemotePath(u))
            case["is_remote_target"] = bool(UriHelper.isRemoteTarget(u))
            case["is_remote_query"] = bool(UriHelper.isRemoteQuery(u))
        cases.append(fix_garbled(case, s))

    for name in NAMES:
        for kind, fn in [
            ("scope", lambda: desc(QueryScopeUri.of(name))),
            ("field", lambda: desc(FieldUri.of(name))),
            ("app_id", lambda: desc(ApplicationUri.of(name, "f"))),
            ("app_field", lambda: desc(ApplicationUri.of("app", name))),
            ("uri_safe", lambda: bool(UriHelper.isUriSafe(name))),
        ]:
            cases.append({"section": "construct", "kind": kind, "name": name, "result": call(fn)})

    for host in HOSTS:
        for port in PORTS:
            for secure in (True, False):
                def build():
                    b = DeephavenTarget.builder().host(host).isSecure(secure)
                    if port is not None:
                        b = b.port(port)
                    return target_desc(b.build())

                cases.append({
                    "section": "target", "host": host, "port": port, "secure": secure, "result": call(build),
                })

    gateway = DeephavenTarget.builder().host("gateway").isSecure(True).build()
    for s in CUSTOM_FOR_REMOTE:
        def remote():
            r = CustomUri.of(create(s)).target(gateway)
            return {"string": r.toString(), "roundtrip": roundtrip(RemoteUri.of, r)}

        cases.append({"section": "custom_remote", "input": s, "result": call(remote)})

    for a, b in EQUAL_PAIRS:
        def uri_eq():
            ua, ub = URI.create(a), URI.create(b)
            return {"equal": bool(ua.equals(ub)), "same_hash": ua.hashCode() == ub.hashCode()}

        def remote_eq():
            return bool(RemoteUri.of(URI.create(a)).equals(RemoteUri.of(URI.create(b))))

        cases.append({"section": "equal", "a": a, "b": b, "uri": call(uri_eq), "remote": call(remote_eq)})

    with open(out_path, "w", encoding="utf-8") as f:
        for case in cases:
            f.write(json.dumps(join_surrogates(case), ensure_ascii=False, sort_keys=True))
            f.write("\n")
    print(f"wrote {len(cases)} cases to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # The JVM keeps non-daemon threads alive; leave without waiting for them.
    os._exit(code)
