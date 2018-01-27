import os

class Helper(object):

    def getRootPath(self):

        return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))