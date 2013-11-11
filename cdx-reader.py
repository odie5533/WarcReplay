# Copyright (c) David Bern


import gzip

import twisted.protocols.basic

CDX_MAPPING = {
    'N': 'massaged_url',
    'b': 'date',
    'a': 'original_url',
    'm': 'mime_type',
    's': 'response_code',
    'k': 'new_style_checksum',
    'r': 'redirect',
    'M': 'meta_tags',
    'S': 'compressed_record_size',
    'V': 'compressed_arc_file_offset',
    'g': 'file_name'
}


class cdx_entry(object):
    def __init__(self, fields):
        f = lambda n: fields[n] if n in fields else None
        self.massaged_url = f('N')
        self.date = f('b')
        self.original_url = f('a')
        self.mime_type = f('m')
        self.response_code = f('s')
        self.new_style_checksum = f('k')
        self.redirect = f('r')
        self.meta_tags = f('M')
        self.compressed_record_size = f('S')
        self.compressed_arc_file_offset = f('V')
        self.file_name = f('g')

    def tostring(self, field_order):
        f = lambda n: getattr(self, CDX_MAPPING[n]) if n in CDX_MAPPING else '-'
        return ' '.join([f(k) for k in field_order])


def field_order_tostring(field_order):
    return ' CDX ' + ' '.join(field_order)


class cdx_reader(twisted.protocols.basic.LineOnlyReceiver):
    def __init__(self):
        self.field_order = None
        self.cdx_entries = []

    def parse_file(self, filename, use_gz=False):
        o = gzip.open if use_gz or filename.endswith('.gz') else open
        with o(filename, 'rb') as f:
            map(self.lineReceived, f.readlines())

    def lineReceived(self, line):
        line = line.strip()
        if self.field_order is None:
            self.field_order = self.parseFieldOrder(line)
            if len(self.field_order) > len(set(self.field_order)):
                print "CDX Header has duplicate keys"
        else:
            e = self.parseEntryLine(self.field_order, line)
            self.entryReceived(e)

    def entryReceived(self, e):
        self.cdx_entries.append(e)

    @staticmethod
    def parseFieldOrder(line):
        p = line.split()
        if p[0] != 'CDX':
            raise ValueError("CDX header does not start with 'CDX'")
        return p[1:]

    @staticmethod
    def parseEntryLine(field_order, line):
        return cdx_entry(dict(zip(field_order, line.split())))

if __name__ == '__main__':
    import argparse

    arg_parser = argparse.ArgumentParser(description="CDX Reader")
    arg_parser.add_argument('-f', '--file', default='out.cdx.gz',
                            help='CDX file to load')
    parsed_args = arg_parser.parse_args()

    cdxr = cdx_reader()
    cdxr.parse_file(parsed_args.file)
