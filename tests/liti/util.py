class NoRaise:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        return True
