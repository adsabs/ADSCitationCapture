import requests
import json
from adsputils import setup_logging
import adsmsg

logger = setup_logging(__name__)

def _build_data(event_type, original_relationship_name, source_bibcode, target_doi):
    data = {
        "event_type": event_type,
        "payload": [
            {
                "source": {
                    "identifier": {
                        "id_url": "http://adsabs.harvard.edu/abs/",
                        "id": source_bibcode,
                        "id_schema": "bibcode"
                    }
                },
                "relationship_type": {
                    "scholix_relationship": "references",
                    "original_relationship_name": original_relationship_name,
                    "original_relationship_schema": "DataCite"
                },
                "target": {
                    "type": {
                        "name": "software"
                    },
                    "identifier": {
                        "id_url": "https://doi.org",
                        "id": target_doi,
                        "id_schema": "DOI"
                    }
                },
                "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
            }
        ],
    }
    return data

def _source_cites_target(citation_change, deleted=False):
    if deleted:
        event_type = "relation_deleted"
    else:
        event_type = "relation_created"
    original_relationship_name = "Cites"
    source_bibcode = citation_change.citing
    target_doi = citation_change.doi
    data = _build_data(event_type, original_relationship_name, source_bibcode, target_doi)
    return data

def _source_is_identical_to_target(citation_change, deleted=False):
    if deleted:
        event_type = "relation_deleted"
    else:
        event_type = "relation_created"
    original_relationship_name = "IsIdenticalTo"
    source_bibcode = citation_change.cited
    target_doi = citation_change.doi
    data = _build_data(event_type, original_relationship_name, source_bibcode, target_doi)
    return data

def _to_data(citation_change):
    if citation_change.status == adsmsg.Status.new:
        return _source_cites_target(citation_change, deleted=False)
    elif citation_change.status == adsmsg.Status.updated and citation_change.cited != '...................':
        return _source_is_identical_to_target(citation_change)
    elif citation_change.status == adsmsg.Status.deleted:
        return _source_cites_target(citation_change, deleted=True)
    else:
        logger.error("Citation change does not match any defined events: {}".format(citation_change))
        return {}

def emit_event(ads_webhook_url, ads_webhook_auth_token, citation_change, timeout=30):
    data = _to_data(citation_change)
    if data:
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer {}".format(ads_webhook_auth_token)
        r = requests.post(ads_webhook_url, data=json.dumps(data), headers=headers, timeout=timeout)
        if not r.ok:
            raise Exception("HTTP Post to '{}' failed: {}".format(ads_webhook_url, json.dumps(data)))
        return True
    return False
