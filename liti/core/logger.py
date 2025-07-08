from logging import Logger


class NoOpLogger(Logger):
    def __init__(self):
        super().__init__('no-op')

    def setLevel(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass

    def critical(self, *args, **kwargs):
        pass

    def fatal(self, *args, **kwargs):
        pass

    def log(self, *args, **kwargs):
        pass

    def findCaller(self, *args, **kwargs):
        return None, None, None, None

    def makeRecord(self, *args, **kwargs):
        pass

    def _log(self, *args, **kwargs):
        pass

    def handle(self, *args, **kwargs):
        pass

    def addHandler(self, *args, **kwargs):
        pass

    def removeHandler(self, *args, **kwargs):
        pass

    def hasHandlers(self, *args, **kwargs):
        pass

    def callHandlers(self, *args, **kwargs):
        pass

    def getEffectiveLevel(self, *args, **kwargs):
        pass

    def isEnabledFor(self, *args, **kwargs):
        return False

    def getChild(self, *args, **kwargs):
        pass

    def getChildren(self, *args, **kwargs):
        pass
