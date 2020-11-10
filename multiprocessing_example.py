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
import urllib.request

from utils import ROSRate


class Server(SyncManager):
    
    counter = 0
    job_q = queue.Queue()
    result_q = queue.Queue()

    def __init__(self, port, authkey, ip_type='local'):
        if ip_type == 'public':
            self.ip = urllib.request.urlopen('https://api.ipify.org/').read().decode('utf8')
        elif ip_type == 'primary': # could be public or private (if behind NAT) 
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
            time.sleep(0.01)

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

Server.register('get_job_q', callable=lambda: Server.job_q)
Server.register('get_result_q', callable=lambda: Server.result_q)


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
        print('started %d processes'%(len(procs)))

        rate = ROSRate(1)

        done_procs = []
        failed_procs = []
        while len(done_procs) + len(failed_procs) != len(procs):
            for p in procs:
                if (not p.is_alive()) and (p not in done_procs):
                    p.join(0)
                    if p.exitcode == 0: done_procs.append(p)
                    else: failed_procs.append(p)
            rate.sleep()
            print('%d processes running (done: %d, failed: %d)'%(len(procs) - len(done_procs), len(done_procs), len(failed_procs)))

Client.register('get_job_q')
Client.register('get_result_q')


class Manager(SyncManager):
    pass

Manager.register('get_job_q')
Manager.register('get_result_q')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    list_of_identities = ['server', 'client']
    parser.add_argument('--identity', type=str,
        help='Specify agent identity. Available identities are %s'%list_of_identities
    )
    parser.add_argument('--config', type=str,
        help='Configuration file'
    )
    args = parser.parse_args()
    assert args.identity in list_of_identities, 'Invalid agent identity. Available identities are %s'%list_of_identities
    
    config_path = 'configs/%s'%(args.config)
    path_to_config = str((Path(__file__).parent / config_path).resolve())
    with open(path_to_config, 'r') as f:
        params = json.load(f, object_hook=lambda d : SimpleNamespace(**d))
    if args.identity == 'server':
        server = Server(params.port, str.encode(params.authkey), ip_type=getattr(params,'ip_type','local'))
        server.start()
        stime = time.time()
        results = server.run()
        ftime = time.time()
        print('number of results:', len(results))
        print('elapsed time:', ftime - stime, 'seconds')
    elif args.identity == 'client':
        client = Client(params.ip, params.port, str.encode(params.authkey))
        client.connect()
        client.run()