import subprocess

import os
import signal

import sys
import time

import psutil

fd = open('tmplog.log', 'w')
worker = subprocess.Popen([sys.executable, "print_to_stderr.py"],
                          stdout=fd,
                          stderr=subprocess.PIPE)


time.sleep(2)


# worker.send_signal(signal.SIGINT)
if sys.platform == 'win32':
    killsig = signal.CTRL_C_EVENT
else:
    killsig = signal.SIGINT

os.kill(worker.pid, killsig)
time.sleep(4)
output, error = worker.communicate()
print(worker.pid)

print(output)
print(error)
print(worker.returncode)

fd.close()