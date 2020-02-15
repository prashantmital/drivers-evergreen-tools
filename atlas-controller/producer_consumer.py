from multiprocessing import Process, Queue


def consumers(queue, process_num):
    for info in iter(queue.get, 'STOP'):
        print("Process #{}. Data: {}".format(process_num, info))


numprocs = 4
shared_queue = Queue()

for i in range(numprocs):
    Process(target=consumers, args=(shared_queue, i)).start()

shared_queue.put('foo')
shared_queue.put('bar')
shared_queue.put('baz')
shared_queue.put('tar')
shared_queue.put('gun')
shared_queue.put('mern')

for i in range(numprocs):
    shared_queue.put('STOP')
