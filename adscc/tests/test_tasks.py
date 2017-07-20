import sys
import os
import json

import unittest
from adscc import app, tasks


class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        self._app = tasks.app
        self.app = app.ADSCitationCaptureCelery('test', proj_home=self.proj_home, local_config={})
        tasks.app = self.app # monkey-patch the app object

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()
        tasks.app = self._app


    def test_task_check_citation(self):
        message = {}
        tasks.task_check_citation(message)
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
