from click import ClickException
from requests import HTTPError


class TestOrchestratorError(ClickException):
    pass


class ResourceAlreadyExistsError(HTTPError):
    pass


class ResourceAlreadyRequestedError(HTTPError):
    pass


class ResourceNotFoundError(HTTPError):
    pass