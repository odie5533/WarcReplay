# Copyright (c) David Bern


import unittest
from cdxreader import cdx_reader, cdx_entry


class test_cdx_reader(unittest.TestCase):
    def test_parseFieldOrder(self):
        fo = ' CDX N b a m s k r M S V g'
        self.assertEqual(cdx_reader.parseFieldOrder(fo),
                         ['N', 'b', 'a', 'm', 's', 'k',
                          'r', 'M', 'S', 'V', 'g'])

    def test_parseFieldOrder_nocdx(self):
        fo = ' N b a m s k r M S V g'
        self.assertRaises(ValueError, cdx_reader.parseFieldOrder, fo)

    def test_parseFieldOrder_nostr(self):
        self.assertRaises(IndexError, cdx_reader.parseFieldOrder, '')
        self.assertRaises(AttributeError, cdx_reader.parseFieldOrder, None)

    def test_parser_good(self):
        fo = ' CDX N b a m s k r M S V g'
        o = '1 2 3 4 5 6 7 8 9 10 11'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k r M S V g'.split())
        c.lineReceived(o)
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order), o)

    def test_parser_duplicate(self):
        fo = ' CDX N b a m s M r M V V N'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s M r M V V N'.split())
        c.lineReceived('1 2 3 4 5 6 7 8 9 10 11')
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '11 2 3 4 5 8 7 8 10 10 11')

    def test_parser_short_field_order(self):
        fo = ' CDX N b a m s k'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k'.split())
        c.lineReceived('1 2 3 4 5 6 7 8 9 10 11')
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '1 2 3 4 5 6')

    def test_parser_long_field_order(self):
        fo = ' CDX N b a m s k r M S V g'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k r M S V g'.split())
        c.lineReceived('1 2 3 4 5 6 7 8 9')
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '1 2 3 4 5 6 7 8 9 - -')

    def test_parser_fake_fields(self):
        fo = ' CDX N b z m s q r X S V x'
        o = '1 2 3 4 5 6 7 8 9 10 11'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b z m s q r X S V x'.split())
        c.lineReceived(o)
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '1 2 - 4 5 - 7 - 9 10 -')

    def test_parser_sample(self):
        fo = ' CDX N b a m s k r M S V g'
        o = 'warcinfo:/wikipedia.warc.gz/archive-commons.0.0.1-SNAPSHOT-20120' \
            '2102659-python 20131109194250 warcinfo:/wikipedia.warc.gz/archiv' \
            'e-commons.0.0.1-SNAPSHOT-20120112102659-python warc-info - 2IGTQ' \
            'CWS2K2D3QYFZZZUCMIHHVSXMYGU - - 338 0 wikipedia.warc.gz'
        c = cdx_reader()
        c.lineReceived(fo)
        c.lineReceived(o)
        e = c.cdx_entries[0]
        self.assertIsInstance(e, cdx_entry)
        self.assertEqual(e.tostring(c.field_order), o)
        self.assertEqual(e.massaged_url, 'warcinfo:/wikipedia.warc.gz/archive-c'
                         'ommons.0.0.1-SNAPSHOT-201202102659-python')
        self.assertEqual(e.date, '20131109194250')
        self.assertEqual(e.original_url, 'warcinfo:/wikipedia.warc.gz/archive-c'
                         'ommons.0.0.1-SNAPSHOT-20120112102659-python')
        self.assertEqual(e.mime_type, 'warc-info')
        self.assertEqual(e.response_code, '-')
        self.assertEqual(e.new_style_checksum,
                         '2IGTQCWS2K2D3QYFZZZUCMIHHVSXMYGU')
        self.assertEqual(e.redirect, '-')
        self.assertEqual(e.meta_tags, '-')
        self.assertEqual(e.compressed_record_size, '338')
        self.assertEqual(e.compressed_arc_file_offset, '0')
        self.assertEqual(e.file_name, 'wikipedia.warc.gz')

if __name__ == '__main__':
    unittest.main()
