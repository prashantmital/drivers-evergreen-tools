from setuptools import setup


setup(
    name='astrolabe',
    version='0.0.1',
    py_modules=['astrolabe'],
    install_requires=['click', 'requests', 'pymongo', 'dnspython', 'pyyaml',
                      'tabulate', 'junitparser'],
    entry_points={'console_scripts': ['astrolabe=astrolabe.cli:cli']})
