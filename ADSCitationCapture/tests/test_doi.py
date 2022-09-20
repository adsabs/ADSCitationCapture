import os
import re
import json
import unittest
import httpretty
from pyingest.parsers.datacite import DataCiteParser
from ADSCitationCapture import app, tasks
from ADSCitationCapture import doi
from .test_base import TestBase


class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)

    def test_software_doi(self):
        doi_id = "10.5281/zenodo.11020" # software
        expected_response_content = self.mock_data[doi_id]['raw']
        expected_parsed_response = self.mock_data[doi_id]['parsed']
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        raw_metadata = doi.fetch_metadata(self.app.conf['DOI_URL'], self.app.conf['DATACITE_URL'], doi_id)
        parsed_metadata = doi.parse_metadata(raw_metadata)
        self.assertEqual(raw_metadata, expected_response_content)
        self.assertEqual(parsed_metadata, expected_parsed_response)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history

    def test_sanitize_doi_trailing_characters(self):
        unsanitized_id = "10.5281/zenodo.11020__amp__quot;__amp__gt" # software
        doi_id = "10.5281/zenodo.11020" # software
        self.assertEqual(doi.sanitize_zenodo_doi(unsanitized_id), doi_id)

    def test_sanitize_doi_conjoined_dois(self):
        unsanitized_id = "10.5281/zenodo.1102010.5281/zenodo.11020" # software
        doi_id = "10.5281/zenodo.11020" # software
        self.assertEqual(doi.sanitize_zenodo_doi(unsanitized_id), doi_id)
    
    def test_sanitize_doi_slash_in_dois(self):
        unsanitized_id = "10.5281/zenodo/11020" # software
        doi_id = "10.5281/zenodo.11020" # software
        self.assertEqual(doi.sanitize_zenodo_doi(unsanitized_id), doi_id)

    def test_non_software_doi(self):
        doi_id = "10.1016/j.dsr2.2008.10.030" # Not software
        expected_response_content = ''
        expected_parsed_response = {}
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        raw_metadata = doi.fetch_metadata(self.app.conf['DOI_URL'], self.app.conf['DATACITE_URL'], doi_id)
        parsed_metadata = doi.parse_metadata(raw_metadata)
        self.assertEqual(raw_metadata, expected_response_content)
        self.assertEqual(parsed_metadata, expected_parsed_response)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history

    def test_decode_datacite_content(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/datacite.json")
        expected_decoded_content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/datacite_decoded.xml")
        with open(content_filename, "r") as f:
            content = "".join(f.readlines())
        with open(expected_decoded_content_filename, "r") as f:
            expected_decoded_content = "".join(f.readlines())
        decoded_content = doi._decode_datacite_content(content)
        self.assertEqual(decoded_content, expected_decoded_content)

    def test_parse_metadata(self):
        datacite_xml_format_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/datacite_decoded.xml")
        with open(datacite_xml_format_filename, "r") as f:
            raw_metadata = "".join(f.readlines())
        datacite_parsed_metadata_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/datacite_parsed_metadata.json")
        with open(datacite_parsed_metadata_filename, "r") as f:
            expected_parsed_metadata = json.loads("".join(f.readlines()))
        dc = DataCiteParser()
        parsed_metadata = dc.parse(raw_metadata)
        self.assertEqual(parsed_metadata, expected_parsed_metadata)

    def test_build_bibcode(self):
        expected_bibcode = "2007zndo.....48535G"
        datacite_parsed_metadata_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/datacite_parsed_metadata_and_authors.json")
        with open(datacite_parsed_metadata_filename, "r") as f:
            parsed_metadata = json.loads("".join(f.readlines()))
        zenodo_bibstem = "zndo"
        zenodo_doi_re = re.compile(r"^10.\d{4,9}/zenodo\.([0-9]*)$", re.IGNORECASE)
        bibcode = doi.build_bibcode(parsed_metadata, zenodo_doi_re, zenodo_bibstem)
        self.assertEqual(bibcode, expected_bibcode)

        expected_bibcode = "2007zndo.....48535."
        # Add forbidden bibcode character as first initial
        parsed_metadata['normalized_authors'][0] = "4" + parsed_metadata['normalized_authors'][0]
        bibcode = doi.build_bibcode(parsed_metadata, zenodo_doi_re, zenodo_bibstem)
        self.assertEqual(bibcode, expected_bibcode)
    
    def test_fetch_all_versions_doi(self):
        expected_output = self.mock_data["10.5281/zenodo.4475376"]["versions"]
        parsed_metadata = self.mock_data["10.5281/zenodo.4475376"]["parsed"]
        output = doi.fetch_all_versions_doi(self.app.conf['DOI_URL'], self.app.conf['DATACITE_URL'], parsed_metadata)
        self.assertEqual(expected_output,output)



if __name__ == '__main__':
    unittest.main()
