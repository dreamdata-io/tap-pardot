from typing import Optional


class TapPardotException(Exception):

    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code

    def __str__(self) -> str:
        return f'{self.code}: {super().__str__()}'


class TapPardotUnorderedDataException(TapPardotException):

    def __init__(self, message: str):
        super().__init__(message, "DATA_OUT_OF_ORDER")


class TapPardotGatewayTimeoutException(TapPardotException):

    def __init__(self, message: str):
        super().__init__(message, "GATEWAY_TIMEOUT")