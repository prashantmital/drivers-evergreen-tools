from urllib.parse import urljoin

from requests import auth, delete, get, patch, post
from requests.exceptions import HTTPError

from astrolabe.exceptions import (
    ResourceAlreadyExistsError, ResourceAlreadyRequestedError,
    ResourceNotFoundError)


def _errored_response_has(exception, status_code=None, error_code=None):
    """Check if the response that raised `exception` bears HTTP status code
    `status_code`, and Atlas error labe `error_code`."""
    # Avoid false positives if user passes no values to check.
    if status_code is None and error_code is None:
        return False

    # Extract response from exception.
    response = exception.response

    # HTTP status code check.
    status_check = True
    if status_code is not None:
        status_check = response.status_code == status_code

    # Atlas error code check
    error_check = True
    if error_code is not None:
        error_check = response.json().get('errorCode') == error_code

    return status_check and error_check

class _RequestBuilder:
    def __init__(self, app_config):
        self.baseurl = app_config.baseurl
        self.auth = auth.HTTPDigestAuth(app_config.apiusername,
                                        app_config.apipassword)
        self.timeout = app_config.httptimeout

    def _build_url(self, path_template, path_parameters):
        if not path_template:
            return self.baseurl
        resource_path = path_template.format(
            **path_parameters)
        return urljoin(self.baseurl, resource_path)

    def _http_method(self, method, path_template=None, path_parameters={},
                     query_parameters={}, body_parameters={}, error=True):
        response = method(self._build_url(path_template, path_parameters),
                          auth=self.auth,
                          params=query_parameters,
                          json=body_parameters,
                          timeout=self.timeout)
        if error:
            response.raise_for_status()
        return response

    def _delete(self, *args, **kwargs):
        return self._http_method(delete, *args, **kwargs)

    def _get(self, *args, **kwargs):
        return self._http_method(get, *args, **kwargs)

    def _patch(self, *args, **kwargs):
        return self._http_method(patch, *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._http_method(post, *args, **kwargs)


class Root(_RequestBuilder):
    def ping(self):
        return self._get()


class Organizations(_RequestBuilder):
    def list(self):
        return self._get(path_template='orgs')

    def get_one_by_name(self, org_name):
        response = self.list()
        for org in response.json()['results']:
            if org['name'] == org_name:
                return org

        raise ResourceNotFoundError(org_name)


class Projects(_RequestBuilder):
    def list(self):
        return self._get(path_template='groups')

    def get_one_by_name(self, group_name):
        try:
            return self._get(path_template='groups/byName/{group_name}',
                             path_parameters={'group_name': group_name})
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=401,
                                     error_code='NOT_IN_GROUP'):
                raise ResourceNotFoundError(exc)
            raise

    def create(self, group_name, org_id):
        try:
            return self._post(
                path_template='groups',
                body_parameters={'name': group_name, 'orgId': org_id})
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=409,
                                     error_code='GROUP_ALREADY_EXISTS'):
                raise ResourceAlreadyExistsError(exc)
            raise


class IPWhitelist(_RequestBuilder):
    PATH_TEMPLATE = 'groups/{group_id}/whitelist'

    def add(self, group_id, ip_details_list):
        self._post(path_template=self.PATH_TEMPLATE,
                   path_parameters={"group_id": group_id},
                   body_parameters=ip_details_list)


class Users(_RequestBuilder):
    PATH_TEMPLATE = 'groups/{group_id}/databaseUsers/'

    def create(self, user_details):
        try:
            return self._post(path_template=self.PATH_TEMPLATE,
                              path_parameters={
                                  'group_id': user_details["groupId"]},
                              body_parameters=user_details)
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=409,
                                     error_code='USER_ALREADY_EXISTS'):
                raise ResourceAlreadyExistsError(exc)
            raise

    def update(self, username, user_details):
        path_template = urljoin(urljoin(self.PATH_TEMPLATE, "admin/"),
                                "{username}")
        return self._patch(path_template=path_template,
                           path_parameters={
                               'group_id': user_details["groupId"],
                               'username': username},
                           body_parameters=user_details)

    def list(self, group_id):
        return self._get(path_template=self.PATH_TEMPLATE,
                         path_parameters={'group_id': group_id})


class Clusters(_RequestBuilder):
    def create(self, group_id, cluster_config):
        try:
            return self._post(path_template='groups/{group_id}/clusters',
                              path_parameters={'group_id': group_id},
                              body_parameters=cluster_config)
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=400,
                                     error_code='DUPLICATE_CLUSTER_NAME'):
                raise ResourceAlreadyExistsError(exc)
            else:
                raise

    def delete(self, group_id, cluster_name):
        try:
            return self._delete(
                path_template='groups/{group_id}/clusters/{cluster_name}',
                path_parameters={'group_id': group_id,
                                 'cluster_name': cluster_name})
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=404,
                                     error_code='CLUSTER_NOT_FOUND'):
                raise ResourceNotFoundError(exc)
            if _errored_response_has(
                    exc, status_code=400,
                    error_code='CLUSTER_ALREADY_REQUESTED_DELETION'):
                return ResourceAlreadyRequestedError(exc)
            raise

    def list(self, group_id):
        return self._get(
            path_template='groups/{group_id}/clusters',
            path_parameters={'group_id': group_id})

    def get_one_by_name(self, group_id, cluster_name):
        try:
            return self._get(
                path_template='groups/{group_id}/clusters/{cluster_name}',
                path_parameters={'group_id': group_id,
                                 'cluster_name': cluster_name})
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=404,
                                     error_code='CLUSTER_NOT_FOUND'):
                raise ResourceNotFoundError(exc)
            raise

    def modify(self, group_id, cluster_name, new_cluster_config):
        try:
            return self._patch(
                path_template='groups/{group_id}/clusters/{cluster_name}',
                path_parameters={'group_id': group_id,
                                 'cluster_name': cluster_name},
                body_parameters=new_cluster_config)
        except HTTPError as exc:
            if _errored_response_has(exc, status_code=400,
                                     error_code='INVALID_ATTRIBUTE'):
                raise ResourceNotFoundError(exc)
            raise

    def get_process_args(self, group_id, cluster_name):
        return self._get(
            path_template='groups/{group_id}/clusters/{cluster_name}/processArgs',
            path_parameters={'group_id': group_id,
                             'cluster_name': cluster_name})

    def modify_process_args(self, group_id, cluster_name, new_process_args):
        return self._patch(
            path_template='groups/{group_id}/clusters/{cluster_name}/processArgs',
            path_parameters={'group_id': group_id,
                             'cluster_name': cluster_name},
            body_parameters=new_process_args)
