import json
import sys
import yaml

import click
from collections import defaultdict
from pprint import pprint

import astrolabe.atlasapi as atlas
from astrolabe.config import (
    AppConfig, DEFAULT_ATLAS_ORGANIZATION, DEFAULT_STATUSPOLLINGTIMEOUT,
    DEFAULT_STATUSPOLLINGFREQUENCY, DEFAULT_DBPASSWORD, DEFAULT_DBUSERNAME,
    PROJECTNAME_ENVVAR, CLUSTERNAME_ENVVAR, CLUSTERNAMESALT_ENVVAR)
from astrolabe.exceptions import (
    ResourceAlreadyExistsError, ResourceAlreadyRequestedError,
    ResourceNotFoundError, TestOrchestratorError)
from astrolabe.utils import APMTestPlan


# Infinitely nested defaultdict type.
def _nested_defaultdict():
    return defaultdict(_nested_defaultdict)


# Utility to merge a list of dictionaries.
def _merge_dictionaries(dicts):
    result = {}
    for d in dicts:
        result.update(d)
    return result


# Custom Click-type for user-input of Atlas Configurations.
class _JsonDotNotationType(click.ParamType):
    def convert(self, value, param, ctx):
        # Return None and target type without change.
        if value is None or isinstance(value, dict):
            return value

        # Parse the input (of type path.to.namespace=value).
        ns, config_value = value.split("=")
        ns_path = ns.split(".")
        return_value = _nested_defaultdict()

        # Construct dictionary from parsed option.
        pointer = return_value
        for key in ns_path:
            if key == ns_path[-1]:
                pointer[key] = config_value
            else:
                pointer = pointer[key]

        # Convert nested defaultdict into vanilla dictionary.
        return json.loads(json.dumps(return_value))


# Define CLI options used in multiple commands for easy re-use.
DBUSERNAME_OPTION = click.option(
    '--db-username', type=click.STRING, default=DEFAULT_DBUSERNAME,
    help='Database username on the MongoDB instance.')

DBPASSWORD_OPTION = click.option(
    '--db-password', type=click.STRING, default=DEFAULT_DBPASSWORD,
    help='Database password on the MongoDB instance.')

ATLASORGANIZATIONNAME_OPTION = click.option(
    '--org-name', type=click.STRING, required=True,
    default=DEFAULT_ATLAS_ORGANIZATION, help='Name of the Atlas Organization.')

ATLASCLUSTERNAME_OPTION = click.option(
    '--cluster-name', required=True, type=click.STRING,
    envvar=CLUSTERNAME_ENVVAR, help='Name of the Atlas Cluster.')

ATLASGROUPNAME_OPTION = click.option(
    '--group-name', type=click.STRING, required=True,
    envvar=PROJECTNAME_ENVVAR, help='Name of the Atlas Project.')

CLUSTERSTATUSPOLLINGTIMEOUT_OPTION = click.option(
    '--polling-timeout', default=DEFAULT_STATUSPOLLINGTIMEOUT,
    type=click.FLOAT,
    help='Maximum time (in seconds) to poll cluster state.')

CLUSTERSTATUSPOLLINGFREQUENCY_OPTION = click.option(
    '--polling-frequency', default=DEFAULT_STATUSPOLLINGFREQUENCY,
    type=click.FLOAT, help='Frequency (in Hz) of polling cluster state.')


@click.group()
@click.version_option()
@click.option('--api-base-url', envvar=AppConfig.ENVVARS["baseurl"],
              type=click.STRING, help='Base URL for the MongoDB Atlas API.')
@click.option('-u', '--api-username', required=True,
              envvar=AppConfig.ENVVARS['apiusername'], type=click.STRING,
              help='HTTP Digest username for authenticating Atlas API access.')
@click.option('-p', '--api-password', required=True,
              envvar=AppConfig.ENVVARS['apipassword'], type=click.STRING,
              help='HTTP Digest password for authenticating Atlas API access.')
@click.option('--http-timeout', envvar=AppConfig.ENVVARS['httptimeout'],
              type=click.INT,
              help='Timeout for HTTP requests to the Atlas API.')
@click.option('-v', '--verbose', count=True, help="Enable HTTP logging.")
@click.pass_context
def cli(ctx, api_base_url, api_username, api_password, http_timeout,
        verbose):
    """
    Astrolabe is a command-line application for running automated driver
    tests against a MongoDB Atlas cluster undergoing maintenance.
    """
    config = AppConfig(
        baseurl=api_base_url,
        apiusername=api_username,
        apipassword=api_password,
        httptimeout=http_timeout,
        verbose=verbose)
    ctx.obj = config


@cli.command()
@click.pass_context
def check_connection(ctx):
    """Command to verify validity of Atlas API credentials."""
    response = atlas.Root(ctx.obj).ping()
    pprint(response.json())

#
# @cli.command()
# @click.option('-c', '--config', multiple=True, type=_JsonDotNotationType())
# def cluster_config_option(config):
#     pprint(_merge_dictionaries(config))


@cli.group('organizations')
def atlas_organizations():
    """Commands related to Atlas Organizations."""
    pass


@atlas_organizations.command('list')
@click.pass_context
def list_all_organizations(ctx):
    """List all Atlas Organizations (limited to first 100)."""
    response = atlas.Organizations(ctx.obj).list()
    pprint(response.json())


@atlas_organizations.command('get-one')
@ATLASORGANIZATIONNAME_OPTION
@click.pass_context
def get_one_organization_by_name(ctx, org_name):
    """Get one Atlas Organization."""
    try:
        pprint(atlas.Organizations(ctx.obj).get_one_by_name(org_name))
    except ResourceNotFoundError:
        raise TestOrchestratorError("Organization {!r} not found".format(
            org_name))


@cli.group('projects')
def atlas_projects():
    """Commands related to Atlas Projects."""
    pass


@atlas_projects.command('create')
@ATLASORGANIZATIONNAME_OPTION
@ATLASGROUPNAME_OPTION
@click.pass_context
def create_project(ctx, org_name, group_name,):
    """Create a new Atlas Project."""
    try:
        org = atlas.Organizations(ctx.obj).get_one_by_name(org_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Organization {!r} not found".format(
            org_name))

    try:
        response = atlas.Projects(ctx.obj).create(
            group_name=group_name, org_id=org['id'])
    except ResourceAlreadyExistsError:
        raise TestOrchestratorError("Project {!r} already exists".format(
            group_name))

    pprint(response.json())


@atlas_projects.command('list')
@click.pass_context
def list_projects(ctx):
    """List all Atlas Projects (limited to first 100)."""
    response = atlas.Projects(ctx.obj).list()
    pprint(response.json())


@atlas_projects.command('get-one')
@ATLASGROUPNAME_OPTION
@click.pass_context
def get_one_project_by_name(ctx, group_name):
    """Get one Atlas Project."""
    try:
        response = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    pprint(response.json())


@cli.group('users')
def atlas_users():
    """Commands related to Atlas Users."""
    pass


@atlas_users.command('create')
@DBUSERNAME_OPTION
@DBPASSWORD_OPTION
@ATLASGROUPNAME_OPTION
@click.pass_context
def create_user(ctx, db_username, db_password, group_name):
    """Create an Atlas User with admin privileges."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    user_details = {
        "groupId": group.json()["id"],
        "databaseName": "admin",
        "roles": [{
            "databaseName": "admin",
            "roleName": "atlasAdmin"}],
        "username": db_username,
        "password": db_password}

    try:
        user = atlas.Users(ctx.obj).create(user_details)
    except ResourceAlreadyExistsError:
        raise TestOrchestratorError("User {!r} already exists".format(
            db_username))

    pprint(user.json())


@atlas_users.command('list')
@ATLASGROUPNAME_OPTION
@click.pass_context
def list_users(ctx, group_name):
    """List all Atlas Users."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    users = atlas.Users(ctx.obj).list(group.json()['id'])
    pprint(users.json())


@cli.group('clusters')
def atlas_clusters():
    """Commands related to Atlas Clusters."""
    pass


@atlas_clusters.command('create-dedicated')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.option('-s', '--instance-size-name', required=True,
              type=click.Choice(["M10", "M20"]),
              help="Name of AWS Cluster Tier to provision.")
@click.pass_context
def create_cluster(ctx, group_name, cluster_name, instance_size_name):
    """Create a new dedicated-tier Atlas Cluster."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    cluster_config = {
        'name': cluster_name,
        'clusterType': 'REPLICASET',
        'providerSettings': {
            'providerName': 'AWS',
            'regionName': 'US_WEST_1',
            'instanceSizeName': instance_size_name}}

    try:
        cluster = atlas.Clusters(ctx.obj).create(
            group.json()['id'], cluster_config)
    except ResourceAlreadyExistsError:
        raise TestOrchestratorError(
            "Cluster {!r} already exists. Use resize/modify to "
            "reconfigure it.".format(cluster_name))

    pprint(cluster.json())


@atlas_clusters.command('get-one')
@ATLASCLUSTERNAME_OPTION
@ATLASGROUPNAME_OPTION
@click.pass_context
def get_one_cluster_by_name(ctx, cluster_name, group_name):
    """Get one Atlas Cluster."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    try:
        cluster = atlas.Clusters(ctx.obj).get_one_by_name(
            group.json()['id'], cluster_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Cluster {!r} not found.".format(
            cluster_name))

    pprint(cluster.json())


@atlas_clusters.command('resize-dedicated')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.option('-s', '--instance-size-name', required=True,
              type=click.Choice(["M10", "M20"]),
              help="Target AWS Cluster Tier.")
@click.pass_context
def resize_cluster(ctx, group_name, cluster_name, instance_size_name):
    """Resize an existing dedicated-tier Atlas Cluster."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    new_cluster_config = {
        'clusterType': 'REPLICASET',
        'providerSettings': {
            'providerName': 'AWS',
            'regionName': 'US_WEST_1',
            'instanceSizeName': instance_size_name}}

    try:
        cluster = atlas.Clusters(ctx.obj).modify(
            group.json()['id'], cluster_name, new_cluster_config)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Cluster {!r} not found.".format(
            cluster_name))

    pprint(cluster.json())


@atlas_clusters.command('toggle-js')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.pass_context
def toggle_cluster_javascript(ctx, group_name, cluster_name):
    """Enable/disable server-side javascript for an existing Atlas Cluster."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    try:
        initial_process_args = atlas.Clusters(ctx.obj).get_process_args(
            group.json()["id"], cluster_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Cluster {!r} not found.".format(
            cluster_name))

    target_js_value = not initial_process_args.json()['javascriptEnabled']

    final_process_args = atlas.Clusters(ctx.obj).modify_process_args(
        group.json()["id"], cluster_name,
        {"javascriptEnabled": target_js_value})

    pprint(final_process_args.json())


@atlas_clusters.command('list')
@ATLASGROUPNAME_OPTION
@click.pass_context
def list_clusters(ctx, group_name):
    """List all Atlas Clusters."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    clusters = atlas.Clusters(ctx.obj).list(group.json()['id'])
    pprint(clusters.json())


@atlas_clusters.command('isready')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.pass_context
def isready_cluster(ctx, group_name, cluster_name):
    """Check if the Atlas Cluster is 'IDLE'."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    try:
        cluster = atlas.Clusters(ctx.obj).get_one_by_name(
            group.json()['id'], cluster_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Cluster {!r} not found.".format(
            cluster_name))

    isready = cluster.json()["stateName"] == "IDLE"

    if isready:
        print("True")
        exit(0)
    print("False")
    exit(1)


@atlas_clusters.command('delete')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.pass_context
def delete_cluster(ctx, group_name, cluster_name):
    """Delete the Atlas Cluster."""
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Project {!r} not found".format(
            group_name))

    try:
        atlas.Clusters(ctx.obj).delete(group.json()['id'], cluster_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Cluster {!r} not found.".format(
            cluster_name))
    except ResourceAlreadyRequestedError:
        pass

    print("DONE!")


# @atlas_clusters.command('getlogs')



# @cli.group('run-debug')
# @click.option('-f', '--test')
# def debug_test():
#     """Command group for running orchestrating tests."""
#     pass
#
#

@cli.group('spec-tests')
def spec_tests():
    """Commands related to running APM spec-tests."""
    pass


@spec_tests.command('run-one')
@click.argument("spec_test_file", type=click.Path(
    exists=True, file_okay=True, dir_okay=False, resolve_path=True))
@click.option('-e', '--workload-executor', required=True, type=click.Path(
    exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    help='Absolute or relative path to the workload-executor')
@click.option('--log-dir', required=True, default="logs",
              type=click.Path(resolve_path=True))
@DBUSERNAME_OPTION
@DBPASSWORD_OPTION
@CLUSTERSTATUSPOLLINGTIMEOUT_OPTION
@CLUSTERSTATUSPOLLINGFREQUENCY_OPTION
@click.pass_context
def run_one_test(ctx, spec_tests_directory, workload_executor, db_username,
                 db_password, polling_timeout, polling_frequency):
    pass


@spec_tests.command('run')
@click.argument("spec_tests_directory", type=click.Path(
    exists=True, file_okay=False, dir_okay=True, resolve_path=True))
@click.option('-e', '--workload-executor', required=True, type=click.Path(
    exists=True, file_okay=True, dir_okay=False, resolve_path=True),
    help='Absolute or relative path to the workload-executor')
@DBUSERNAME_OPTION
@DBPASSWORD_OPTION
@ATLASORGANIZATIONNAME_OPTION
@ATLASGROUPNAME_OPTION
@click.option('--cluster-name-salt', type=click.STRING, required=True,
              envvar=CLUSTERNAMESALT_ENVVAR,
              help='Salt used to generate almost-unique Cluster names.')
@CLUSTERSTATUSPOLLINGTIMEOUT_OPTION
@CLUSTERSTATUSPOLLINGFREQUENCY_OPTION
@click.pass_context
def run_headless(ctx, spec_tests_directory, workload_executor, db_username,
                 db_password, org_name, group_name, cluster_name_salt,
                 polling_timeout, polling_frequency):
    """
    Main entry point for running APM tests in headless environments.
    This command runs all tests found in the SPEC_TESTS_DIRECTORY
    sequentially on an Atlas cluster.
    """
    # Step-1: ensure validity of provided Atlas Organization.
    # Organizations can only be created by users manually via the web UI.
    try:
        organization = atlas.Organizations(ctx.obj).get_one_by_name(org_name)
    except ResourceNotFoundError:
        raise TestOrchestratorError("Organization {!r} not found".format(
            org_name))
    else:
        click.echo("Using Atlas Organization {!r}. ID: {!r}".format(
            organization["name"], organization["id"]))

    # Step-2: check that the project exists or else create one.
    try:
        group = atlas.Projects(ctx.obj).get_one_by_name(group_name)
    except ResourceNotFoundError:
        group = atlas.Projects(ctx.obj).create(
            group_name=group_name, org_id=organization['id'])
    finally:
        click.echo("Using Atlas Project {!r}. ID: {!r}".format(
            group.json()["name"], group.json()["id"]))

    # Step-3: create a user under the project and populate the IP whitelist.
    # This user will be used by all tests to run operations.
    # 0.0.0.0 will be added to the IP whitelist enabling "access from anywhere".
    user_details = {
        "groupId": group.json()["id"],
        "databaseName": "admin",
        "roles": [{
            "databaseName": "admin",
            "roleName": "atlasAdmin"}],
        "username": db_username,
        "password": db_password}

    try:
        atlas.Users(ctx.obj).create(user_details)
    except ResourceAlreadyExistsError:
        # Cannot send username when updating an existing user.
        username = user_details.pop("username")
        atlas.Users(ctx.obj).update(username, user_details)
    finally:
        click.echo("Using Atlas User {!r}".format(username))

    ip_details_list = [{"cidrBlock": "0.0.0.0/0"},]
    # TODO catch ResourceAlreadyExistsError here.
    atlas.IPWhitelist(ctx.obj).add(group.json()["id"], ip_details_list)

    # Step-4: create a test-plan.
    # The test-plan is a list of tuples. Each tuple contains:
    #   - the unique test name
    #   - the JSON test-definition loaded as a dictionary
    #   - the unique Atlas cluster name (29 characters max)
    from astrolabe.commands import walk_spec_test_directory
    from astrolabe.utils import APMTest
    from hashlib import sha256
    test_plans = []
    for test_name, test_path in walk_spec_test_directory(spec_tests_directory):
        with open(test_path, 'r') as fp:
            test_spec = yaml.load(fp, Loader=yaml.FullLoader)

        name_hash = sha256(cluster_name_salt.encode('utf-8'))
        name_hash.update(test_name.encode('utf-8'))
        cluster_name = name_hash.hexdigest()[:10]

        test_plans.append(APMTest(test_name, test_spec, cluster_name))

    from tabulate import tabulate
    _cases = []
    for test_case in test_plans:
        _cases.append([test_case.test_name, test_case.cluster_name])
    click.echo("--------------- Test Plan --------------- ")
    click.echo(tabulate(
        _cases, headers=["Test Case Name", "Atlas Cluster Name"],
        showindex="always"))

    # Step-5: initialize all clusters required by the test-plan.
    # Cluster initialization involves the following steps:
    #   - Create cluster with given configuration and name; if a cluster
    #     bearing the desired name already exists, the entire run fails.
    #   - Modify advanced config of cluster to desired initial state
    for test_case in test_plans:
        _, spec, cluster_name = test_case
        config = spec["maintenancePlan"]["initial"]["basicConfiguration"].copy()
        config["name"] = cluster_name

        try:
            atlas.Clusters(ctx.obj).create(group.json()["id"], config)
        except ResourceAlreadyExistsError:
            cluster_name = config.pop("name")
            atlas.Clusters(ctx.obj).modify(group.json()['id'], cluster_name, config)

        process_args = spec["maintenancePlan"]["initial"]["processArgs"]
        if process_args:
            atlas.Clusters(ctx.obj).modify_process_args(
                group.json()["id"], cluster_name, process_args)

    # Step-6: while there are remaining tests to be run,
    #         wait until one of the corresponding clusters is ready.
    is_failure = False

    import junitparser
    junit_suite = junitparser.TestSuite('Atlas Planned Maintenance Testing')

    while test_plans:
        from astrolabe.commands import select_callback, get_ready_test_plan
        test_case, cluster = select_callback(
            get_ready_test_plan,
            (ctx.obj, group.json()["id"], test_plans),
            {},
            polling_frequency,
            polling_timeout)

        # Once the test to be run has been selected, run the test.
        click.echo("Running test {} on cluster {}".format(test_case.test_name, test_case.cluster_name))

        # Test step-1: insert initial/test data
        test_data = test_case.spec["driverWorkload"].get("testData")
        if test_data is not None:
            from pymongo import MongoClient
            client = MongoClient(
                cluster["srvAddress"], username=db_username,
                password=db_password, w="majority")
            coll = client.get_database(
                test_case.spec["driverWorkload"]["database"]).get_collection(
                test_case.spec["driverWorkload"]["collection"])
            coll.drop()
            # import pdb; pdb.set_trace()
            coll.insert_many(test_data)
            click.echo("Loaded test data!")

        # Test step-2: get cmdline args for workload executor
        from astrolabe.commands import get_executor_args
        connection_string, workload_spec = get_executor_args(
            test_case, db_username, db_password, cluster["srvAddress"])

        # Test step-3: start the workload in a subprocess.
        # STDOUT, STDERR of workload is attached to XUNIT output.
        from time import time
        test_start_time = time()

        import subprocess
        worker_subprocess = subprocess.Popen(
            [sys.executable, workload_executor, connection_string, json.dumps(workload_spec)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        click.echo("Workload executor running. PID: {}".format(worker_subprocess.pid))

        # Test step-4: kick-off the maintenance routine using the API
        # This call blocks until maintenance is complete.
        from astrolabe.commands import run_maintenance, is_server_state
        # import pdb; pdb.set_trace()
        run_maintenance(ctx.obj, test_case, group.json()['id'])
        select_callback(
            is_server_state,
            (ctx.obj, group.json()['id'], test_case.cluster_name, "IDLE"),
            {},
            polling_frequency,
            polling_timeout)

        import os
        import signal
        if sys.platform != 'win32':
            sigint = signal.SIGINT
        else:
            sigint = signal.CTRL_C_EVENT

        os.kill(worker_subprocess.pid, sigint)    # might need to do this asynchronously on a thread
        stdout, stderr = worker_subprocess.communicate()
        print(stdout)
        print("---------")
        print(stderr)

        test_end_time = time()

        # Do more stuff:
        # - parse stderr as JSON
        # - write to XUNIT output using junitparser
        # - set is_failure to True if worker_subprocess.returncode is nonzero
        junit_test = junitparser.TestCase(test_name)
        junit_test.time = test_end_time - test_start_time

        if worker_subprocess.returncode != 0:
            is_failure = True
            errmsg = """
            Number of errors: {numErrors}
            Number of failures: {numFailures}    
            """
            try:
                errinfo = json.loads(stderr)
                junit_test.result = junitparser.Failure(errmsg.format(**errinfo))
            except json.JSONDecodeError:
                junit_test.result = junitparser.Error(stderr)
            junit_test.system_err = stdout
        else:
            junit_test.system_out = stdout

        junit_suite.add_testcase(junit_test)

        # Finally:
        test_plans.remove(test_case)

        # TODO
        # download logs and delete cluster asynchronously
        #cleanup_queue.put(cluster_name)    # cleanup queue downloads logs and deletes cluster
        atlas.Clusters(ctx.obj).delete(group.json()["id"], test_case.cluster_name)

    # Step-7: write the XUNIT file
    xml = junitparser.JUnitXml()
    xml.add_testsuite(junit_suite)
    xml.write('junit.xml')

    # Step-8: make a zipfile with all logs
    # import shutil
    # shutil.make_archive(...)

    if is_failure:
        exit(1)
    else:
        exit(0)


if __name__ == '__main__':
    cli()
