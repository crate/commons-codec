class MessageFormatError(Exception):
    pass


class UnknownOperationError(Exception):
    def __init__(self, *args, operation=None, record=None, **kwargs):
        self.operation = operation
        self.record = record
        super().__init__(*args, **kwargs)
