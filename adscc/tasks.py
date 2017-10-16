from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
import adscc.app as app_module
import adscc.webhook as webhook
import adscc.doi as doi
import adscc.url as url
#from adsmsg import CitationUpdate

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCitationCaptureCelery('ads-citation-capture', proj_home=proj_home)
logger = app.logger


app.conf.CELERY_QUEUES = (
    Queue('process-citation-changes', app.exchange, routing_key='process-citation-changes'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)


# ============================= TASKS ============================================= #

@app.task(queue='process-citation-changes')
def task_process_citation_changes(citation_changes):
    """
    Process citation changes
    """
    logger.debug('Checking content: %s', citation_changes)
    for citation_change in citation_changes.changes:
        if citation_change.doi != "":
            # Fetch DOI metadata (if HTTP request fails, an exception is raised
            # and the task will be re-queued (see app.py and adsputils))
            is_software = doi.is_software(app.conf['DOI_URL'], doi)
            is_link_alive = True
        elif citation_change.pid != "":
            is_software = True
            is_link_alive = url.is_alive(app.conf['ASCL_URL'] + citation_change.pid)
        elif citation_change.url != "":
            is_software = False
            is_link_alive = url.is_alive(citation_change.url)
        else:
            is_software = False
            is_link_alive = False
            logger.error("Citation change should have doi, pid or url informed: {}", citation_change)
            #raise Exception("Citation change should have doi, pid or url informed: {}".format(citation_change))

        emitted = False
        if is_software and is_link_alive:
            emitted = webhook.emit_event(app.conf['ADS_WEBHOOK_URL'], app.conf['ADS_WEBHOOK_AUTH_TOKEN'], citation_changes)

        if emitted:
            logger.debug("Emitted '%s'", citation_change)
        else:
            logger.debug("Not emitted '%s'", citation_change)

        #logger.debug("Calling 'task_output_results' with '%s'", citation_change)
        ##task_output_results.delay(citation_change)
        #task_output_results(citation_change)

@app.task(queue='output-results')
def task_output_results(citation_change):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param citation_change: contains citation changes
    :return: no return
    """
    logger.debug('Will forward this record: %s', citation_change)
    logger.debug("Calling 'app.forward_message' with '%s'", str(citation_change))
    app.forward_message(citation_change)


if __name__ == '__main__':
    app.start()
