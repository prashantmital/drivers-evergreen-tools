import requests

from astrolabe.utils import JSONObject


class AtlasAPIException(Exception):
    def __init__(self, msg, resource_url=None, request_method=None,
                 status_code=None, error_code=None, headers=None):
        self._msg = msg
        self.request_method = request_method
        self.resource_url = resource_url
        self.status_code = status_code
        self.error_code = error_code
        self.headers = headers

    def __str__(self):
        if self.request_method and self.resource_url:
            return '{} ({} {})'.format(self._msg, self.request_method,
                                       self.resource_url)
        return self._msg


class AtlasClientError(AtlasAPIException):
    pass


class AtlasAPIError(AtlasAPIException):
    def __init__(self, msg, response=None, request_method=None,
                 error_code=None):
        kwargs = {
            'request_method': request_method,
            'error_code': error_code,
        }
        if response is not None:
            # Parse remaining fields from response object.
            kwargs.update(
                {
                    'status_code': response.status_code,
                    'resource_url': response.url,
                    'headers': response.headers,
                }
            )

        super().__init__(msg, **kwargs)

    def __str__(self):
        if self.request_method and self.resource_url and self.error_code:
            return '{} Error Code: {!r} ({} {})'.format(
                self._msg, self.error_code, self.request_method,
                self.resource_url)
        return super().__str__()


class AtlasRateLimitError(AtlasAPIError):
    pass


_EMPTY_PATH_ERR_MSG_TEMPLATE = ('Calling {} on an empty API path is not '
                                'supported.')


class ApiComponent:
    def __init__(self, client, path=None):
        self._client = client
        self._path = path

    def __repr__(self):
        return '<ApiComponent: %s>' % self._path

    def __getitem__(self, path):
        if self._path is not None:
            path = '%s/%s' % (self._path, path)
        return ApiComponent(self._client, path)

    def __getattr__(self, path):
        return self[path]

    def get(self, **params):
        if self._path is None:
            raise TypeError(_EMPTY_PATH_ERR_MSG_TEMPLATE.format('get()'))
        return self._client.request('GET', self._path, **params)

    def patch(self, **params):
        if self._path is None:
            raise TypeError(_EMPTY_PATH_ERR_MSG_TEMPLATE.format('patch()'))
        return self._client.request('PATCH', self._path, **params)

    def post(self, **params):
        if self._path is None:
            raise TypeError(_EMPTY_PATH_ERR_MSG_TEMPLATE.format('post()'))
        return self._client.request('POST', self._path, **params)

    def delete(self, **params):
        if self._path is None:
            raise TypeError(_EMPTY_PATH_ERR_MSG_TEMPLATE.format('delete()'))
        return self._client.request('DELETE', self._path, **params)

    def get_path(self):
        return self._path


class ApiResponse:
    def __init__(self, response, request_method, json_data):
        self.resource_url = response.url
        self.headers = response.headers
        self.request_method = request_method
        self.data = json_data

    def __repr__(self):
        return '<{}: {} {}>'.format(self.__class__.__name__,
                                     self.request_method, self.resource_url)


class AtlasClient:
    def __init__(self, config):
        self.config = config

    def __getattr__(self, path):
        return ApiComponent(self, path)

    @property
    def root(self):
        """Access the root resource of the Atlas API.

        This needs special handling because empty paths are not generally
        supported by the Fluent API.
        """
        return ApiComponent(self, '')

    def request(self, method, path, **params):
        method = method.upper()
        url = self.construct_resource_url(path)

        query_params = {}
        for param_name in ("pretty", "envelope", "itemsPerPage", "pageNum"):
            if param_name in params:
                query_params[param_name] = params.pop(param_name)

        raw_body_params = params.pop('raw_body_params')
        if raw_body_params:
            params = raw_body_params

        request_kwargs = {
            'auth': self.config.auth,
            'params': query_params,
            'json': params,
            'timeout': self.config.timeout}

        try:
            response = requests.request(method, url, **request_kwargs)
        except requests.RequestException as e:
            raise AtlasClientError(
                str(e),
                resource_url=url,
                request_method=method
            )

        return self.handle_response(method, response)

    def construct_resource_url(self, path):
        url_template = "{base_url}/v{version}/{resource_path}"
        return url_template.format(base_url=self.config.base_url,
                                   version=self.config.api_version,
                                   resource_path=path)

    @staticmethod
    def handle_response(method, response):
        try:
            data = response.json(object_hook=JSONObject)
        except ValueError:
            data = None

        if response.status_code in (200, 201, 202):
            return ApiResponse(response, method, data)

        if response.status_code == 429:
            raise AtlasRateLimitError('Too many requests', response=response,
                                      request_method=method, error_code=429)

        if data is None:
            raise AtlasAPIError('Unable to decode JSON response.',
                                response=response, request_method=method)

        atlas_error_code = data.get('errorCode')
        kwargs = {
            'response': response,
            'request_method': method,
            'error_code': atlas_error_code}

        if response.status_code == 400:
            raise AtlasAPIError('400: Bad Request.', **kwargs)

        if response.status_code == 401:
            raise AtlasAPIError('401: Unauthorized.', **kwargs)

        if response.status_code == 403:
            raise AtlasAPIError('403: Forbidden.', **kwargs)

        if response.status_code == 404:
            raise AtlasAPIError('404: Not Found.', **kwargs)

        if response.status_code == 40:
            raise AtlasAPIError('409: Conflict.', **kwargs)

        raise AtlasAPIError(
            'An unknown error has occured processing your request.', **kwargs)
