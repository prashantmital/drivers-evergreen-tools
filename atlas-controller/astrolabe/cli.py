from pprint import pprint
import json
import subprocess
import sys
from time import sleep
import yaml

import click

from astrolabe.atlas_client import AtlasClient
from astrolabe.exceptions import AtlasApiError
import astrolabe.commands as commands
from astrolabe.config import (
    setup_configuration, CONFIG_DEFAULTS as DEFAULTS,
    CONFIG_ENVVARS as ENVVARS)
from astrolabe.utils import APMTest, Timer


# Define CLI options used in multiple commands for easy re-use.
DBUSERNAME_OPTION = click.option(
    '--db-username', type=click.STRING, default=DEFAULTS.DB_USERNAME,
    help='Database username on the MongoDB instance.')

DBPASSWORD_OPTION = click.option(
    '--db-password', type=click.STRING, default=DEFAULTS.DB_PASSWORD,
    help='Database password on the MongoDB instance.')

ATLASORGANIZATIONNAME_OPTION = click.option(
    '--org-name', type=click.STRING, default=DEFAULTS.ATLAS_ORGANIZATION,
    required=True, help='Name of the Atlas Organization.')

ATLASCLUSTERNAME_OPTION = click.option(
    '--cluster-name', required=True, type=click.STRING,
    help='Name of the Atlas Cluster.')

ATLASGROUPNAME_OPTION = click.option(
    '--group-name', required=True, type=click.STRING,
    envvar=ENVVARS.PROJECT_NAME, help='Name of the Atlas Project.')


@click.group()
@click.option('--atlas-base-url',
              envvar="ATLAS_API_BASE_URL",
              default="https://cloud.mongodb.com/api/atlas",
              type=click.STRING, help='Base URL of the Atlas API.')
@click.option('--atlas-api-version',
              envvar="ATLAS_API_VERSION", default=1.0,
              type=click.types.FLOAT, help="Version of the Atlas API.")
@click.option('-u', '--atlas-api-username', required=True,
              envvar="ATLAS_API_USERNAME",
              type=click.STRING, help='HTTP-Digest username.')
@click.option('-p', '--atlas-api-password', required=True,
              envvar="ATLAS_API_PASSWORD",
              type=click.STRING, help='HTTP-Digest password.')
@click.option('--http-timeout', type=click.FLOAT,
              envvar="ATLAS_HTTP_TIMEOUT", default=10,
              help='Time (in s) after which HTTP requests should timeout.')
@click.option('--polling-timeout', type=click.FLOAT,
              envvar="ATLAS_POLLING_TIMEOUT", default=600.0,
              help="Maximum time (in s) to poll API endpoints.")
@click.option('--polling-frequency', type=click.FLOAT,
              envvar="ATLAS_POLLING_FREQUENCY", default=1.0,
              help='Frequency (in Hz) at which to poll API endpoints.')
@click.option('-v', '--verbose', count=True, default=False,
              help="Set the logging level. Default: off.")
@click.version_option()
@click.pass_context
def cli(ctx, atlas_base_url, atlas_api_version, atlas_api_username,
        atlas_api_password, http_timeout, polling_timeout, polling_frequency,
        verbose):
    """
    Astrolabe is a command-line application for running automated driver
    tests against a MongoDB Atlas cluster undergoing maintenance.
    """
    config = setup_configuration(
        atlas_base_url, atlas_api_version, atlas_api_username,
        atlas_api_password, http_timeout, polling_timeout, polling_frequency,
        verbose)
    client = AtlasClient(config)
    ctx.obj = client


@cli.command()
@click.pass_context
def check_connection(ctx):
    """Command to verify validity of Atlas API credentials."""
    pprint(ctx.obj.root.get().data)

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
    pprint(ctx.obj.orgs.get().data)


@atlas_organizations.command('get-one')
@ATLASORGANIZATIONNAME_OPTION
@click.pass_context
def get_one_organization_by_name(ctx, org_name):
    """Get one Atlas Organization by name. Prints "None" if no organization
    bearing the given name exists."""
    pprint(commands.get_one_organization_by_name(ctx.obj, org_name))


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
    org = commands.get_one_organization_by_name(ctx.obj, org_name)
    response = ctx.obj.groups.post(name=group_name, orgId=org.id)
    pprint(response.data)


@atlas_projects.command('list')
@click.pass_context
def list_projects(ctx):
    """List all Atlas Projects (limited to first 100)."""
    pprint(ctx.obj.groups.get().data)


@atlas_projects.command('get-one')
@ATLASGROUPNAME_OPTION
@click.pass_context
def get_one_project_by_name(ctx, group_name):
    """Get one Atlas Project."""
    pprint(ctx.obj.groups.byName[group_name].get().data)


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
    user = commands.create_admin_user(
        ctx.obj, db_username, db_password, group_name)
    pprint(user.data)


@atlas_users.command('list')
@ATLASGROUPNAME_OPTION
@click.pass_context
def list_users(ctx, group_name):
    """List all Atlas Users."""
    project = ctx.obj.groups.byName[group_name].get().data
    pprint(ctx.obj.groups[project.id].databaseUsers.get().data)


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
    project = ctx.obj.groups.byName[group_name].get().data

    cluster_config = {
        'name': cluster_name,
        'clusterType': 'REPLICASET',
        'providerSettings': {
            'providerName': 'AWS',
            'regionName': 'US_WEST_1',
            'instanceSizeName': instance_size_name}}

    cluster = ctx.obj.groups[project.id].clusters.post(**cluster_config)
    pprint(cluster.data)


@atlas_clusters.command('get-one')
@ATLASCLUSTERNAME_OPTION
@ATLASGROUPNAME_OPTION
@click.pass_context
def get_one_cluster_by_name(ctx, cluster_name, group_name):
    """Get one Atlas Cluster."""
    project = ctx.obj.groups.byName[group_name].get().data
    cluster = ctx.obj.groups[project.id].clusters[cluster_name].get()
    pprint(cluster.data)


@atlas_clusters.command('resize-dedicated')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.option('-s', '--instance-size-name', required=True,
              type=click.Choice(["M10", "M20"]),
              help="Target AWS Cluster Tier.")
@click.pass_context
def resize_cluster(ctx, group_name, cluster_name, instance_size_name):
    """Resize an existing dedicated-tier Atlas Cluster."""
    project = ctx.obj.groups.byName[group_name].get().data

    new_cluster_config = {
        'clusterType': 'REPLICASET',
        'providerSettings': {
            'providerName': 'AWS',
            'regionName': 'US_WEST_1',
            'instanceSizeName': instance_size_name}}

    cluster = ctx.obj.groups[project.id].clusters[cluster_name].patch(
        **new_cluster_config)
    pprint(cluster.data)


@atlas_clusters.command('toggle-js')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.pass_context
def toggle_cluster_javascript(ctx, group_name, cluster_name):
    """Enable/disable server-side javascript for an existing Atlas Cluster."""
    project = ctx.obj.groups.byName[group_name].get().data

    # Alias to reduce verbosity.
    pargs = ctx.obj.groups[project.id].clusters[cluster_name].processArgs

    initial_process_args = pargs.get()
    target_js_value = not initial_process_args.data.javascriptEnabled

    cluster = pargs.patch(javascriptEnabled=target_js_value)
    pprint(cluster.data)


@atlas_clusters.command('list')
@ATLASGROUPNAME_OPTION
@click.pass_context
def list_clusters(ctx, group_name):
    """List all Atlas Clusters."""
    project = ctx.obj.groups.byName[group_name].get().data
    clusters = ctx.obj.groups[project.id].clusters.get()
    pprint(clusters.data)


@atlas_clusters.command('isready')
@ATLASGROUPNAME_OPTION
@ATLASCLUSTERNAME_OPTION
@click.pass_context
def isready_cluster(ctx, group_name, cluster_name):
    """Check if the Atlas Cluster is 'IDLE'."""
    isready = commands.is_cluster_state(
        ctx.obj, group_name, cluster_name, "IDLE")

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
    project = ctx.obj.groups.byName[group_name].get().data
    ctx.obj.groups[project.id].clusters[cluster_name].delete().data
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
              envvar=ENVVARS.CLUSTER_NAME_SALT,
              help='Salt used to generate unique Cluster names.')
@click.pass_context
def run_headless(ctx, spec_tests_directory, workload_executor, db_username,
                 db_password, org_name, group_name, cluster_name_salt,
                 polling_timeout, polling_frequency):
    """
    Main entry point for running APM tests in headless environments.
    This command runs all tests found in the SPEC_TESTS_DIRECTORY
    sequentially on an Atlas cluster.
    """
    # Alias for simplicity.
    client = ctx.obj

    # Step-1: ensure validity of provided Atlas Organization.
    # Organizations can only be created by users manually via the web UI.
    organization = commands.get_one_organization_by_name(client, org_name)
    click.echo("Using Atlas Organization {!r}. ID: {!r}".format(
        organization.name, organization.id))

    # Step-2: check that the project exists or else create one.
    try:
        group = client.groups.byName[group_name].get().data
    except AtlasApiError:
        # Create the group if it doesn't exist.
        group = client.groups.post(name=group_name, orgId=organization.id).data
    click.echo("Using Atlas Project {!r}. ID: {!r}".format(
        group.name, group.id))

    # Step-3: create a user under the project and populate the IP whitelist.
    # This user will be used by all tests to run operations.
    # 0.0.0.0 will be added to the IP whitelist enabling "access from anywhere".
    user_details = {
        "groupId": group.id,
        "databaseName": "admin",
        "roles": [{
            "databaseName": "admin",
            "roleName": "atlasAdmin"}],
        "username": db_username,
        "password": db_password}

    try:
        client.groups[group.id].databaseUsers.post(**user_details)
    except AtlasApiError as exc:
        if exc.error_code == "USER_ALREADY_EXISTS":
            # Cannot send username when updating an existing user.
            username = user_details.pop("username")
            client.groups[group.id].databaseUsers.admin[username].patch(
                **user_details)
        else:
            raise
    click.echo("Using Atlas User {!r}".format(db_username))

    ip_details_list = [{"cidrBlock": "0.0.0.0/0"},]
    # TODO catch ResourceAlreadyExistsError here.
    client.groups[group.id].whitelist.post(json=ip_details_list)

    # Step-4: create a test-plan.
    # The test-plan is a list of tuples. Each tuple contains:
    #   - the unique test name
    #   - the JSON test-definition loaded as a dictionary
    #   - the unique Atlas cluster name (29 characters max)
    from hashlib import sha256
    test_plans = []
    for test_name, test_path in commands.walk_spec_test_directory(
            spec_tests_directory):
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
            client.groups[group.id].clusters.post(**config)
        except AtlasApiError as exc:
            if exc.error_code == 'DUPLICATE_CLUSTER_NAME':
                # Cannot send cluster name when updating existing cluster.
                cluster_name = config.pop("name")
                client.groups[group.id].clusters[cluster_name].patch(**config)

        process_args = spec["maintenancePlan"]["initial"]["processArgs"]
        if process_args:
            client.groups[group.id].clusters[cluster_name].processArgs.patch(
                **process_args)

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
        test_timer = Timer()

        worker_subprocess = subprocess.Popen(
            [sys.executable, workload_executor, connection_string, json.dumps(workload_spec)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        click.echo("Workload executor running. PID: {}".format(worker_subprocess.pid))

        # Test step-4: kick-off the maintenance routine using the API
        # This call blocks until maintenance is complete.
        commands.run_maintenance(ctx.obj, test_case, group.json()['id'])
        sleep(3)     # must sleep here or it is possible to miss maintenance altogether
        select_callback(
            commands.is_cluster_state,
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

        test_timer.stop()

        # Do more stuff:
        # - parse stderr as JSON
        # - write to XUNIT output using junitparser
        # - set is_failure to True if worker_subprocess.returncode is nonzero
        junit_test = junitparser.TestCase(test_case.test_name)
        junit_test.time = test_timer.elapsed

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
        # cleanup_queue.put(cluster_name)    # cleanup queue downloads logs and deletes cluster
        # atlas.Clusters(ctx.obj).delete(group.json()["id"], test_case.cluster_name)

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
