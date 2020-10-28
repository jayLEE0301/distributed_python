# reference: https://eli.thegreenplace.net/2012/01/24/distributed-computing-in-python-with-multiprocessing
import argparse
from functools import reduce
import json
import multiprocessing
from multiprocessing.managers import SyncManager
from pathlib import Path
import queue
import socket
import time
from types import SimpleNamespace


# def make_server_manager(port, authkey):
#     """ Create a manager for the server, listening on the given port.
#         Return a manager object with get_job_q and get_result_q methods.
#     """
#     job_q = Queue.Queue()
#     result_q = Queue.Queue()

#     # This is based on the examples in the official docs of multiprocessing.
#     # get_{job|result}_q return synchronized proxies for the actual Queue
#     # objects.
#     class JobQueueManager(SyncManager):
#         pass

#     JobQueueManager.register('get_job_q', callable=lambda: job_q)
#     JobQueueManager.register('get_result_q', callable=lambda: result_q)

#     manager = JobQueueManager(address=('', port), authkey=authkey)
#     manager.start()
#     print 'Server started at port %s' % port
#     return manager


# def make_nums(N):
#     """ Create N large numbers to factorize.
#     """
#     nums = [999999999999]
#     for i in xrange(N):
#         nums.append(nums[-1] + 2)
#     return nums


# def runserver():
#     # Start a shared manager server and access its queues
#     manager = make_server_manager(PORTNUM, AUTHKEY)
#     shared_job_q = manager.get_job_q()
#     shared_result_q = manager.get_result_q()

#     N = 999
#     nums = make_nums(N)

#     # The numbers are split into chunks. Each chunk is pushed into the job
#     # queue.
#     chunksize = 43
#     for i in range(0, len(nums), chunksize):
#         shared_job_q.put(nums[i:i + chunksize])

#     # Wait until all results are ready in shared_result_q
#     numresults = 0
#     resultdict = {}
#     while numresults < N:
#         outdict = shared_result_q.get()
#         resultdict.update(outdict)
#         numresults += len(outdict)

#     # Sleep a bit before shutting down the server - to give clients time to
#     # realize the job queue is empty and exit in an orderly way.
#     time.sleep(2)
#     manager.shutdown()


# def runclient():
#     manager = make_client_manager(IP, PORTNUM, AUTHKEY)
#     job_q = manager.get_job_q()
#     result_q = manager.get_result_q()
#     mp_factorizer(job_q, result_q, 4)


# def make_client_manager(ip, port, authkey):
#     """ Create a manager for a client. This manager connects to a server on the
#         given address and exposes the get_job_q and get_result_q methods for
#         accessing the shared queues from the server.
#         Return a manager object.
#     """
#     class ServerQueueManager(SyncManager):
#         pass

#     ServerQueueManager.register('get_job_q')
#     ServerQueueManager.register('get_result_q')

#     manager = ServerQueueManager(address=(ip, port), authkey=authkey)
#     manager.connect()

#     print 'Client connected to %s:%s' % (ip, port)
#     return manager


job_q = queue.Queue()
result_q = queue.Queue()

class Server(SyncManager):
    
    counter = 0

    def __init__(self, port, authkey, ip_type='global'):
        if ip_type == 'global':
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('1.1.1.1', 53))
            self.ip = s.getsockname()[0]
            s.close()
        elif ip_type == 'local':
            self.ip = '127.0.0.1'
        else:
            raise ValueError('Invalid ip_type')
        self.port = port
        super(Server, self).__init__(address=(self.ip, self.port), authkey=authkey)
        print('Configured server')
        Server.counter += 1
        if Server.counter > 1:
            raise Exception('More than one instance of Server already exist! (existing instances: %d)'%(Server.counter))

    def start(self, *args, **kwargs):
        super(Server, self).start(*args, **kwargs)
        print('Server opened at %s:%s'%(self.ip,self.port))

    def run(self):
        # Start a shared manager server and access its queues
        shared_job_q = self.get_job_q()
        shared_result_q = self.get_result_q()
        
        N = 999
        nums = Server._make_nums(N)

        # The numbers are split into chunks. Each chunk is pushed into the job
        # queue.
        chunksize = 43
        for i in range(0, len(nums), chunksize):
            shared_job_q.put(nums[i:i + chunksize])

        # Wait until all results are ready in shared_result_q
        numresults = 0
        resultdict = {}
        while numresults < N:
            try:
                outdict = shared_result_q.get_nowait()
                resultdict.update(outdict)
                numresults += len(outdict)
                print('%d out of %d jobs done'%(numresults,N))
            except queue.Empty:
                pass
            time.sleep(1)

        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(2)
        self.shutdown()
        print('All jobs done')

        return resultdict

    @staticmethod
    def _make_nums(N):
        nums = [999999999999]
        for _ in range(N):
            nums.append(nums[-1] + 2)
        return nums

    def __del__(self):
        Server.counter -= 1

Server.register('get_job_q', callable=lambda: job_q)
Server.register('get_result_q', callable=lambda: result_q)


class Client(SyncManager):

    def __init__(self, ip, port, authkey):
        self.ip = ip
        self.port = port
        super(Client, self).__init__(address=(ip, port), authkey=authkey)
        print('Configured client')

    def connect(self, *args, **kwargs):
        super(Client, self).connect(*args, **kwargs)
        print('Client connected to %s:%s'%(self.ip,self.port))

    def run(self):
        job_q = self.get_job_q()
        result_q = self.get_result_q()
        Client.mp_factorizer(job_q, result_q, 4)

    @staticmethod
    def factorize_naive(n):
        return set(reduce(list.__add__, ([i, n//i] for i in range(1, int(n**0.5) + 1) if n % i == 0)))

    @staticmethod
    def factorizer_worker(job_q, result_q):
        """ A worker function to be launched in a separate process. Takes jobs from
            job_q - each job a list of numbers to factorize. When the job is done,
            the result (dict mapping number -> list of factors) is placed into
            result_q. Runs until job_q is empty.
        """
        while True:
            try:
                job = job_q.get_nowait()
                outdict = {n: Client.factorize_naive(n) for n in job}
                result_q.put(outdict)
            except queue.Empty:
                return

    @staticmethod
    def mp_factorizer(shared_job_q, shared_result_q, nprocs):
        """ Split the work with jobs in shared_job_q and results in
            shared_result_q into several processes. Launch each process with
            factorizer_worker as the worker function, and wait until all are
            finished.
        """
        procs = []
        for i in range(nprocs):
            p = multiprocessing.Process(
                    target=Client.factorizer_worker,
                    args=(shared_job_q, shared_result_q))
            procs.append(p)
            p.start()

        for p in procs:
            p.join()

Client.register('get_job_q')
Client.register('get_result_q')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    list_of_identities = ['server', 'client']
    parser.add_argument('--identity', type=str,
        help='Specify agent identity. Available identities are %s'%list_of_identities
    )
    args = parser.parse_args()
    assert args.identity in list_of_identities, 'Invalid agent identity. Available identities are %s'%list_of_identities
    
    json_filename = 'configs/%s_example.json'%(args.identity)
    path_to_config = str((Path(__file__).parent / json_filename).resolve())
    with open(path_to_config, 'r') as f:
        params = json.load(f, object_hook=lambda d : SimpleNamespace(**d))
    if args.identity == 'server':
        server = Server(params.port, str.encode(params.authkey), ip_type='local')
        server.start()
        results = server.run()
        print(results)
    elif args.identity == 'client':
        client = Client(params.ip, params.port, str.encode(params.authkey))
        client.connect()
        client.run()