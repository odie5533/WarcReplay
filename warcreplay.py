# Copyright (c) 2013 David Bern


import argparse

from twisted.internet import reactor, protocol
from twisted.web.client import _URI

from hanzo.warctools import WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

from TwistedWebProxyServer import WebProxyServerProtocol

class MetaRecordInfo:
    """
    Stores information needed to retrieve a WARC Record
    This functions similar to a CDX entry
    """
    def __init__(self, uri, offset, rtype, filename):
        self.uri = uri
        self.offset = offset
        self.rtype = rtype
        self.filename = filename
        
    def uriEquals(self, uri, ignoreScheme=False):
        self_uri = _URI.fromBytes(self.uri)
        comp_uri = _URI.fromBytes(uri)
        if ignoreScheme:
            self_uri.scheme = comp_uri.scheme = None
        return self_uri.toBytes() == comp_uri.toBytes()
        

class WarcReplayHandler:
    def __init__(self):
        self.metaRecords = []
        self.responseMetaRecords = []
    
    @staticmethod
    def loadWarcFileRecords(name):
        """ Generator function for records from the file 'name' """
        f = WarcRecord.open_archive(name, gzip="auto")
        for (offset, r, err) in f.read_records(limit=None):
            if err:
                print "warc errors at %s:%d" % (name, offset or 0)
                for e in err:
                    print '\t', e
            if r:
                yield (r, offset)
        f.close()
        
    def loadWarcFile(self, name):
        for r, off in self.loadWarcFileRecords(name):
            i = MetaRecordInfo(r.url, off, r.type, name)
            if r.type == WarcRecord.RESPONSE:
                self.responseMetaRecords.append(i)
            self.metaRecords.append(i)
    
    def recordFromUri(self, uri):
        p = [m for m in self.responseMetaRecords if m.uriEquals(uri, ignoreScheme=True)]
        if len(p) < 1:
            return None
        return self.readRecord(p[0].filename, p[0].offset)
    
    @staticmethod
    def readRecord(filename, offset):
        w = WarcRecord.open_archive(filename, offset=offset)
        g = w.read_records(limit=1)
        r = g.next()[1]
        w.close()
        return r

def _copy_attrs(to, frum, attrs):
    map(lambda a: setattr(to, a, getattr(frum, a)), attrs)

class WarcReplayProtocol(WebProxyServerProtocol):
    def __init__(self, wrp, *args, **kwargs):
        WebProxyServerProtocol.__init__(self, *args, **kwargs)
        self._wrp = wrp
        
    @staticmethod
    def getRecordUri(request_uri, connect_uri):
        req_uri = _URI.fromBytes(request_uri)
        con_uri = _URI.fromBytes(connect_uri)
        # Remove default port from URL
        if con_uri.port == (80 if con_uri.scheme == 'http' else 443):
            con_uri.netloc = con_uri.host
        # Copy parameters from the relative req_uri to the con_uri
        _copy_attrs(con_uri, req_uri, ['path','params','query','fragment'])
        return con_uri.toBytes()
    
    def writeRecordToTransport(self, r, t):
        m = ResponseMessage(RequestMessage())
        m.feed(r.content[1])
        m.close()        
        b = m.get_body()
        
        # construct new headers
        new_headers = []
        old_headers = []
        for k, v in m.header.headers:
            if not k.lower() in ("connection", "content-length",
                                 "cache-control","accept-ranges", "etag",
                                 "last-modified", "transfer-encoding"):
                new_headers.append((k, v))
            old_headers.append(("X-Archive-Orig-%s" % k, v))
        
        new_headers.append(("Content-Length", "%d" % len(b)))
        new_headers.append(("Connection", "keep-alive"))
        # write the response
        t.write("%s %d %s\r\n" % (m.header.version,
                                  m.header.code,
                                  m.header.phrase))
        h = new_headers + old_headers
        t.write("\r\n".join(["%s: %s" % (k, v) for k, v in h]))
        t.write("\r\n\r\n")
        t.write(b)
    
    def requestParsed(self, request):
        record_uri = self.getRecordUri(request.uri, self.connect_uri)
        #print "requestParsed:", record_uri
        r = self._wrp.recordFromUri(record_uri)
        
        if r is not None:
            self.writeRecordToTransport(r, self.transport)
        else:
            print "404: ", record_uri
            resp = "URL not found in archives."
            self.transport.write("HTTP/1.0 404 Not Found\r\n"\
                                 "Connection: keep-alive\r\n"\
                                 "Content-Type: text/plain\r\n"\
                                 "Content-Length: %d\r\n\r\n"\
                                 "%s\r\n" % (len(resp)+2, resp))

class ReplayServerFactory(protocol.ServerFactory):
    protocol = WarcReplayProtocol
    
    def __init__(self, warcFiles=[]):
        self._wrp = WarcReplayHandler()
        for n in warcFiles:
            self._wrp.loadWarcFile(n)
    
    def buildProtocol(self, addr):
        p = self.protocol(self._wrp)
        p.factory = self
        return p

if __name__=='__main__':
    parser = argparse.ArgumentParser(
                             description='WarcReplay')
    parser.add_argument('-p', '--port', default='1080',
                        help='Port to run the proxy server on.')
    parser.add_argument('-w', '--warc', default='out.warc.gz',
                        help='WARC file to load')
    args = parser.parse_args()
    args.port = int(args.port)

    rsf = ReplayServerFactory(warcFiles=[args.warc])
    reactor.listenTCP(args.port, rsf)
    print "Proxy running on port", args.port
    reactor.run()
