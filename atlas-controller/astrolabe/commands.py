import os

from time import sleep, time

from click import echo, Abort

import astrolabe.exceptions
from astrolabe.exceptions import TestOrchestratorError, AtlasClientError, AtlasApiError, AtlasRateLimitError
from astrolabe.utils import assert_subset


def get_one_organization_by_name(client, org_name):
    all_orgs = client.orgs.get().data
    for org in all_orgs.results:
        if org.name == org_name:
            return org
    raise AtlasApiError('Resource not found.')


def create_admin_user(client, username, password, group_name):
    project = client.groups.byName[group_name].get().data
    user_details = {
        "groupId": project.id,
        "databaseName": "admin",
        "roles": [{
            "databaseName": "admin",
            "roleName": "atlasAdmin"}],
        "username": username,
        "password": password}
    return client.groups[project.id].databaseUsers.post(**user_details)


def get_cluster_state(client, group_name, cluster_name):
    project = client.groups.byName[group_name].get().data
    cluster = client.groups[project.id].clusters[cluster_name].get().data
    return cluster.stateName


def is_cluster_state(client, group_name, cluster_name, target_state):
    current_state = get_cluster_state(client, group_name, cluster_name)
    return current_state == target_state


def wait_until_cluster_state(client, group_name, cluster_name, target_state,
                             polling_frequency, polling_timeout):
    if is_cluster_state(client, group_name, cluster_name,
                        target_state):
        return True

    start_time = time()
    sleep_interval = 1 / polling_frequency
    while (time() - start_time) < polling_timeout:
        if is_cluster_state(client, group_name, cluster_name, target_state):
            return True
        sleep(sleep_interval)

    return False


def select_callback(callback, args, kwargs, frequency, timeout):
    start_time = time()
    interval = 1 / frequency
    while (time() - start_time) < timeout:
        return_value = callback(*args, **kwargs)
        if return_value is not None:
            return return_value
        print("Waiting {} seconds before retrying".format(interval))
        sleep(interval)
    raise RuntimeError      # TODO make new error type for polling timeout


def get_ready_test_plan(client, group_id, test_plans):
    clusters = client.groups[group_id].clusters
    for test_case in test_plans:
        cluster_resource = clusters[test_case.cluster_name]
        cluster = cluster_resource.get().data
        if cluster.stateName == "IDLE":
            # Verification
            assert_subset(cluster, test_case.spec["maintenancePlan"]["initial"]["basicConfiguration"])
            processArgs = cluster_resource.processArgs.get().data
            assert_subset(processArgs, test_case.spec["maintenancePlan"]["initial"]["processArgs"])
            print("Cluster {} is ready!".format(test_case.cluster_name))
            return test_case, cluster
        else:
            print("Cluster {} is not ready!".format(test_case.cluster_name))
    return None


def get_executor_args(test_case, username, password, plain_srv_address):
    prefix, suffix = plain_srv_address.split("//")

    srv_address = prefix + "//" + username + ":" + password + "@" + suffix + "/?"

    uri_options = test_case.spec["maintenancePlan"]["uriOptions"]

    from urllib.parse import urlencode
    srv_address = srv_address + urlencode(uri_options)

    return srv_address, test_case.spec["driverWorkload"]


def run_maintenance(client, test_case, group_id):
    final_config = test_case.spec["maintenancePlan"]["final"]

    basic_conf = final_config["basicConfiguration"]
    process_args = final_config["processArgs"]

    if not basic_conf and not process_args:
        raise RuntimeError("invalid maintenance plan - both configs cannot be blank")

    cluster = client.groups[group_id].clusters[test_case.cluster_name]
    if basic_conf:
        cluster.patch(**basic_conf)

    if process_args:
        cluster.processArgs.patch(**process_args)

    print("Maintenance has been started!")


def walk_spec_test_directory(testdir):
    for testfile in os.listdir(testdir):
        fullpath = os.path.abspath(os.path.join(testdir, testfile))
        if not os.path.isfile(fullpath):
            continue
        fname, _ = os.path.splitext(os.path.split(fullpath)[1])
        testname = fname.replace('-', '_')
        yield testname, fullpath
