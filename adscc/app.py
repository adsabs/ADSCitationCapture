from adsputils import ADSCelery
from models import *

class ADSCitationCaptureCelery(ADSCelery):
    def attempt_recovery(self, task, args=None, kwargs=None, einfo=None, retval=None):
        """
        If task fails, keep trying forever
        """
        task.apply_async(args=args, kwargs=kwargs)
