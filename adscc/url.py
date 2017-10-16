import requests
from adsputils import setup_logging

logger = setup_logging(__name__)

def is_alive(url):
    try:
        request = requests.get(url)
    except:
        logger.exception("Failed URL: %s", url)
        raise
    return request.ok
