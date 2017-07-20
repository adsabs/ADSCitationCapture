from __future__ import absolute_import, unicode_literals
import os
from kombu import Queue
import adscc.app as app_module

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCitationCaptureCelery('ads-citation-capture', proj_home=proj_home)
logger = app.logger


app.conf.CELERY_QUEUES = (
    Queue('check-citation', app.exchange, routing_key='check-if-extract'),
)


# ============================= TASKS ============================================= #

@app.task(queue='check-citation')
def task_check_citation(message):
    """
    Checks if the citation needs to be processed
    """
    logger.debug('Checking content: %s', message)
    pass



if __name__ == '__main__':
    app.start()
