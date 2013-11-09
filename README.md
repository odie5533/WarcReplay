WarcReplay
==========
WarcReplay lets you view the contents of a WARC file by simply browsing websites
in a web browser. It creates an HTTP(S) proxy which you set your browser to
connect to. Then any time you try to access a web site, WarcReplay will send
your browser the already archived contents of the website you are trying to
access.

Prerequisites
=============
WarcReplay requires the [Twisted networking library](http://twistedmatrix.com/trac/).
The library can be installed by running `pip install twisted` or by installing
binaries which are available on their website. 

Usage
=====
To run WarcReplay, execute:

    python warcreplay.py

Then point your browser to connect to a proxy. The default proxy settings are
IP: 127.0.0.1  Port: 1080

To view all the command line options, run the script with the option `--help`.
