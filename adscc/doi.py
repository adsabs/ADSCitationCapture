import requests
import json
from lxml import etree
from adsputils import setup_logging

logger = setup_logging(__name__)

def is_software(base_doi_url, doi):
    doi_endpoint = base_doi_url + doi
    headers = {}
    ## Supported content types: https://citation.crosscite.org/docs.html#sec-4
    #headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1, application/vnd.crossref.unixref+xml;q=1"
    #headers["Accept"] = "application/vnd.crossref.unixref+xml;q=1" # This format does not contain software type tag
    headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1"
    data = {}
    timeout = 30
    try:
        r = requests.get(doi_endpoint, data=json.dumps(data), headers=headers, timeout=timeout)
    except:
        logger.exception("HTTP request to DOI service failed: %s", doi_endpoint)
        success = False
    else:
        success = True

    if not success or not r.ok:
        logger.error("HTTP request to DOI service failed: %s", doi_endpoint)
        raise Exception("HTTP request to DOI service failed: {}".format(doi_endpoint))

    is_software = False
    try:
        root = etree.fromstring(r.content)
    except:
        pass
    else:
        resource_type = root.find("{http://datacite.org/schema/kernel-3}resourceType")
        if resource_type is not None:
            resource_type_general = resource_type.get('resourceTypeGeneral')
            is_software = resource_type_general is not None and resource_type_general.lower() == "software"
    return is_software
