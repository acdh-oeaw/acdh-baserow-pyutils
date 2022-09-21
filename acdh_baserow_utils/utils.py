class BaseRowUtilsError(Exception):
    """Base class for other exceptions"""

    def __init__(self, msg="No Baserow Token was set"):
        self.message = msg
        super().__init__(self.message)
