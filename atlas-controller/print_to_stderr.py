import sys


try:
    while True:
        pass
except KeyboardInterrupt:
    print("{x: true}", file=sys.stderr)
    print("foo")
    exit(1)


