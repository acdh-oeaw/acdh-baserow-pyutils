class NoTokenFound(Exception):
    """Base class for other exceptions"""

    def __init__(self):
        self.message = "No Baserow Token was set"
        super().__init__(self.message)
