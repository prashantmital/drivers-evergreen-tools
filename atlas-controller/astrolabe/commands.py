import os

from time import sleep, time

from click import echo, Abort

import astrolabe.atlasapi as atlas
import astrolabe.exceptions
from astrolabe.exceptions import TestOrchestratorError
from astrolabe.utils import assert_subset


def get_group_by_name_safe(config, group_name):
    try:
        group = atlas.Projects(config).get_one_by_name(group_name)
    except astrolabe.exceptions.ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))
    return group.json()


def get_organization_by_name(config, org_name):
    response = atlas.Organizations(config).list()
    for org in response.json()['results']:
        if org['name'] == org_name:
            return org
    return None


def create_admin_user(config, username, password, group_name):
    group = atlas.Projects(config).get_one_by_name(group_name).json()
    user_details = {
        "databaseName": "admin",
        "roles": [{
            "databaseName": "admin",
            "roleName": "atlasAdmin"}],
        "username": username,
        "password": password}
    return atlas.Users(config).create(
        group_id=group['id'], user_details=user_details)


def get_cluster_state(config, group_name, cluster_name):
    group = atlas.Projects(config).get_one_by_name(group_name).json()
    cluster = atlas.Clusters(config).get_one_by_name(group['id'],
                                                      cluster_name)
    return cluster.json()["stateName"]


def is_cluster_state(config, group_name, cluster_name, target_state):
    current_state = get_cluster_state(config, group_name, cluster_name)
    return current_state == target_state


def wait_until_cluster_state(config, group_name, cluster_name, target_state,
                             polling_frequency, polling_timeout):
    if is_cluster_state(config, group_name, cluster_name,
                        target_state):
        return True

    start_time = time()
    sleep_interval = 1 / polling_frequency
    while (time() - start_time) < polling_timeout:
        if is_cluster_state(config, group_name, cluster_name, target_state):
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


def is_server_state(config, group_id, cluster_name, target_state):
    cluster = atlas.Clusters(config).get_one_by_name(group_id, cluster_name).json()
    if cluster["stateName"] == target_state:
        return cluster
    else:
        print("Server is not in goal state. Current state: {}".format(cluster["stateName"]))
        return None


def get_ready_test_plan(config, group_id, test_plans):
    for test_case in test_plans:
        cluster = atlas.Clusters(config).get_one_by_name(
            group_id, test_case.cluster_name).json()
        if cluster["stateName"] == "IDLE":
            # Verification
            assert_subset(cluster, test_case.spec["maintenancePlan"]["initial"]["basicConfiguration"])
            processArgs = atlas.Clusters(config).get_process_args(group_id, test_case.cluster_name).json()
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


def run_maintenance(config, test_case, group_id):
    final_config = test_case.spec["maintenancePlan"]["final"]

    basic_conf = final_config["basicConfiguration"]
    process_args = final_config["processArgs"]

    if not basic_conf and not process_args:
        raise RuntimeError("invalid maintenance plan - both configs cannot be blank")

    if basic_conf:
        atlas.Clusters(config).modify(group_id, test_case.cluster_name, basic_conf)

    if process_args:
        atlas.Clusters(config).modify_process_args(group_id, test_case.cluster_name, process_args)

    print("Maintenance has been started!")


def walk_spec_test_directory(testdir):
    for testfile in os.listdir(testdir):
        fullpath = os.path.abspath(os.path.join(testdir, testfile))
        if not os.path.isfile(fullpath):
            continue
        fname, _ = os.path.splitext(os.path.split(fullpath)[1])
        testname = fname.replace('-', '_')
        yield testname, fullpath
