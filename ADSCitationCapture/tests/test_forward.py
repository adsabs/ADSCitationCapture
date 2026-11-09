import sys
import os
import json
import tempfile
import adsmsg
from ADSCitationCapture import webhook
from ADSCitationCapture import doi
from ADSCitationCapture import url
from ADSCitationCapture import db
from ADSCitationCapture import forward
from adsmsg import DenormalizedRecord
from .test_base import TestBase

import unittest
from ADSCitationCapture import app, tasks
from mock import patch


class TestWorkers(TestBase):

    def _common_citation_changes_doi(self, status):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2005CaJES..42.1987P'
        citation_change.cited = '...................'
        citation_change.content = '10.5281/zenodo.11020'
        citation_change.content_type = adsmsg.CitationChangeContentType.doi
        citation_change.resolved = False
        citation_change.status = status
        return citation_changes

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)
        
    def test_build_bib_record_no_associated_works(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_bib_record.json")
        with open(content_filename) as f:
            expect_bib_record = json.load(f)
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_nonbib_record.json")
        with open(content_filename) as f:
            expect_nonbib_record = json.load(f)

        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        doi_id = "10.5281/zenodo.11020" # software
        parsed_metadata = self.mock_data[doi_id]['parsed']
        citations =['']
        db_versions = {"":""}
        bib_record, nonbib_record = forward.build_record(self.app, citation_change, parsed_metadata, citations, db_versions)  
        self.assertEqual(bib_record.toJSON(),expect_bib_record)
        self.assertEqual(nonbib_record.toJSON(),expect_nonbib_record)

    def test_build_bib_record_orcid(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_bib_record_orcid.json")
        with open(content_filename) as f:
            expect_bib_record = json.load(f)
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_nonbib_record_orcid.json")
        with open(content_filename) as f:
            expect_nonbib_record = json.load(f)

        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        doi_id = "10.5281/zenodo.10683242" # software
        parsed_metadata = self.mock_data[doi_id]['parsed']
        citations =['']
        db_versions = {"":""}
        bib_record, nonbib_record = forward.build_record(self.app, citation_change, parsed_metadata, citations, db_versions)  
        self.assertEqual(bib_record.toJSON(),expect_bib_record)
        self.assertEqual(nonbib_record.toJSON(),expect_nonbib_record)

    def test_build_bib_record_associated_works(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_bib_record_associated.json")
        with open(content_filename) as f:
            expect_bib_record = json.load(f)
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_nonbib_record_associated.json")
        with open(content_filename) as f:
            expect_nonbib_record = json.load(f)

        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        doi_id = "10.5281/zenodo.4475376" # software
        parsed_metadata = self.mock_data[doi_id]['parsed']
        citations =['']
        db_versions = self.mock_data[doi_id]['associated']
        bib_record, nonbib_record = forward.build_record(self.app, citation_change, parsed_metadata, citations, db_versions)  
        
        self.assertEqual(bib_record.toJSON(),expect_bib_record)
        self.assertEqual(nonbib_record.toJSON(),expect_nonbib_record)

    def _build_record_with_hssi_links(self, links_filename):
        self.app.conf['HSSI_LINKS_FILE'] = links_filename
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        parsed_metadata = self.mock_data["10.5281/zenodo.11020"]['parsed']
        return forward.build_record(self.app, citation_change, parsed_metadata, [''], {"":""})

    def test_build_record_hssi_links(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            links_filename = os.path.join(tmp_dir, 'hssi_links.tsv')
            with open(links_filename, 'w') as f:
                f.write('10.5281/ZENODO.11020\thttps://hssi.hsdcloud.org/software/b/\n'
                        '10.5281/zenodo.11020\thttps://hssi.hsdcloud.org/software/a/\n'
                        '10.5281/zenodo.11020\thttps://hssi.hsdcloud.org/software/a/\n'
                        '10.5281/zenodo.1\thttps://hssi.hsdcloud.org/software/c/\n'
                        'invalid line\n\n')
            bib_record, nonbib_record = self._build_record_with_hssi_links(links_filename)

        urls = ['https://hssi.hsdcloud.org/software/a/', 'https://hssi.hsdcloud.org/software/b/']
        self.assertIn('DATA', bib_record.property)
        self.assertIn('DATA', nonbib_record.property)
        self.assertEqual(['HSSI:2'], nonbib_record.toJSON()['data'])
        self.assertEqual(2, nonbib_record.total_link_counts)
        self.assertEqual(['ESOURCE', 'DATA'], [row.link_type for row in nonbib_record.data_links_rows])
        hssi_row = nonbib_record.data_links_rows[1]
        self.assertEqual('HSSI', hssi_row.link_sub_type)
        self.assertEqual(urls, list(hssi_row.url))
        self.assertEqual(['', ''], list(hssi_row.title))
        self.assertEqual(2, hssi_row.item_count)

    def test_build_record_doi_without_hssi_links(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            links_filename = os.path.join(tmp_dir, 'hssi_links.tsv')
            with open(links_filename, 'w') as f:
                f.write('10.5281/zenodo.1\thttps://hssi.hsdcloud.org/software/c/\n')
            bib_record, nonbib_record = self._build_record_with_hssi_links(links_filename)

        self.assertNotIn('DATA', nonbib_record.property)
        self.assertNotIn('data', nonbib_record.toJSON())
        self.assertEqual(['ESOURCE'], [row.link_type for row in nonbib_record.data_links_rows])

    def test_build_record_hssi_links_file_reloaded(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            links_filename = os.path.join(tmp_dir, 'hssi_links.tsv')
            with open(links_filename, 'w') as f:
                f.write('10.5281/zenodo.11020\thttps://hssi.hsdcloud.org/software/a/\n')
            self._build_record_with_hssi_links(links_filename)
            with open(links_filename, 'w') as f:
                f.write('10.5281/zenodo.1\thttps://hssi.hsdcloud.org/software/c/\n')
            os.utime(links_filename, (1, 1))
            bib_record, nonbib_record = self._build_record_with_hssi_links(links_filename)

        self.assertNotIn('data', nonbib_record.toJSON())

    def test_build_record_missing_hssi_links_file(self):
        with self.assertRaises(OSError):
            self._build_record_with_hssi_links('/nonexistent/hssi_links.tsv')

if __name__ == '__main__':
    unittest.main()
