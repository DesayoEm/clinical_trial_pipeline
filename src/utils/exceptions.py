class CTPException(Exception):
    """ Base class for all CTPExceptions exceptions"""


class FailedRequestError(CTPException):
    def __init__(self, current_page: int, detail: str):
        super().__init__()
        self.log = f"Request failed on page {current_page}.Details: {detail}. DETAIL:{detail}"


class NextPageError(CTPException):
    def __init__(self, page: int):
        self.log = f"Next page not found on page {page}"


class FileCompactionError(CTPException):
    def __init__(self, details: str):
        self.log = f"File compaction failed {details}"


class MissingStateError(CTPException):
    def __init__(self, state: str):
        self.log = f"{state} could not be determined"
