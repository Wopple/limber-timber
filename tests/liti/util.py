class NoRaise:
    def __enter__(self):
        pass

    def __exit__(self, _exc_type, _exc_value, _traceback):
        return True
