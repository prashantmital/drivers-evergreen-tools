from collections import namedtuple

from tabulate import tabulate


def assert_subset(dict1, dict2):
    """Utility that asserts that `dict2` is a subset of `dict1`, while
    accounting for nested fields."""
    for key, value in dict2.items():
        if key not in dict1:
            raise AssertionError("not a subset")
        if isinstance(value, dict):
            assert_subset(dict1[key], value)
        else:
            assert dict1[key] == value


APMTest = namedtuple("APMTest", ["test_name", "spec", "cluster_name"])


class APMTestPlan:
    """Dataclass to represent an APM Test Plan."""
    def __init__(self):
        self._names = []
        self._specs = []
        self._clusters = []
        self._status = []

    def __len__(self):
        return len(self._names)

    # def __iter__(self):
    #     for

    def add_test_case(self, name, spec, cluster, status=None):
        self._names.append(name)
        self._specs.append(spec)
        self._clusters.append(cluster)
        self._status.append(status or "INIT")

    def __str__(self):
        test_list = []
        for test_case in self:
            test_list.append()
        return tabulate(
            [self._names, self._clusters, self._status],
            headers=["Test Name", "Cluster Name", "Test Status"])
