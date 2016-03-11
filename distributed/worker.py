from __future__ import print_function, division, absolute_import

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from importlib import import_module
import logging
from multiprocessing.pool import ThreadPool
import os
import pkg_resources
import tempfile
from timeit import default_timer
import traceback
import shutil
import sys

from dask.core import istask
from dask.compatibility import apply
from toolz import merge, valmap, assoc
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.queues import Queue

from .client import pack_data, gather_from_workers
from .compatibility import reload, PY3, unicode
from .core import (rpc, Server, pingpong, dumps, loads, coerce_to_address,
        error_message, read, write)
from .sizeof import sizeof
from .utils import (funcname, get_ip, get_traceback, truncate_exception,
    ignoring, _maybe_complex, log_errors)

_ncores = ThreadPool()._processes


logger = logging.getLogger(__name__)


class Worker(Server):
    """ Worker Node

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Additionally workers keep a Center informed of their data and use that
    Center to gather data from other workers when necessary to perform a
    computation.

    You can start a worker with the ``dworker`` command line application::

        $ dworker scheduler-ip:port

    **State**

    * **data:** ``{key: object}``:
        Dictionary mapping keys to actual values
    * **active:** ``{key}``:
        Set of keys currently under computation
    * **ncores:** ``int``:
        Number of cores used by this worker process
    * **executor:** ``concurrent.futures.ThreadPoolExecutor``:
        Executor used to perform computation
    * **local_dir:** ``path``:
        Path on local machine to store temporary files
    * **center:** ``rpc``:
        Location of center or scheduler.  See ``.ip/.port`` attributes.
    * **name:** ``string``:
        Alias
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:

    Examples
    --------

    Create centers and workers in Python:

    >>> from distributed import Center, Worker
    >>> c = Center('192.168.0.100', 8787)  # doctest: +SKIP
    >>> w = Worker(c.ip, c.port)  # doctest: +SKIP
    >>> yield w._start(port=8788)  # doctest: +SKIP

    Or use the command line::

       $ dcenter
       Start center at 127.0.0.1:8787

       $ dworker 127.0.0.1:8787
       Start worker at:            127.0.0.1:8788
       Registered with center at:  127.0.0.1:8787

    See Also
    --------
    distributed.center.Center:
    """

    def __init__(self, center_ip, center_port, ip=None, ncores=None,
                 loop=None, local_dir=None, services=None, service_ports=None,
                 name=None, **kwargs):
        self.ip = ip or get_ip()
        self._port = 0
        self.ncores = ncores or _ncores
        self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self.local_dir = local_dir or tempfile.mkdtemp(prefix='worker-')
        self.executor = ThreadPoolExecutor(self.ncores)
        self.center = rpc(ip=center_ip, port=center_port)
        self.active = set()
        self.name = name

        if not os.path.exists(self.local_dir):
            os.mkdir(self.local_dir)

        if self.local_dir not in sys.path:
            sys.path.insert(0, self.local_dir)

        self.services = {}
        self.service_ports = service_ports or {}
        for k, v in (services or {}).items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            self.services[k] = v(self)
            self.services[k].listen(port)
            self.service_ports[k] = self.services[k].port

        handlers = {'compute': self.compute,
                    'gather': self.gather,
                    'compute-stream': self.compute_stream,
                    'run': self.run,
                    'get_data': self.get_data,
                    'update_data': self.update_data,
                    'delete_data': self.delete_data,
                    'terminate': self.terminate,
                    'ping': pingpong,
                    'upload_file': self.upload_file}

        super(Worker, self).__init__(handlers, **kwargs)

    @gen.coroutine
    def _start(self, port=0):
        self.listen(port)
        self.name = self.name or self.address
        for k, v in self.services.items():
            v.listen(0)
            self.service_ports[k] = v.port

        logger.info('      Start worker at: %20s:%d', self.ip, self.port)
        for k, v in self.service_ports.items():
            logger.info('  %16s at: %20s:%d' % (k, self.ip, v))
        logger.info('Waiting to connect to: %20s:%d',
                    self.center.ip, self.center.port)
        while True:
            try:
                resp = yield self.center.register(
                        ncores=self.ncores, address=(self.ip, self.port),
                        keys=list(self.data), services=self.service_ports,
                        name=self.name)
                break
            except (OSError, StreamClosedError):
                logger.debug("Unable to register with scheduler.  Waiting")
                yield gen.sleep(0.5)
        if resp != 'OK':
            raise ValueError(resp)
        logger.info('        Registered to: %20s:%d',
                    self.center.ip, self.center.port)
        self.status = 'running'

    def start(self, port=0):
        self.loop.add_callback(self._start, port)

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id,
                'center': (self.center.ip, self.center.port)}

    @gen.coroutine
    def _close(self, report=True, timeout=10):
        if report:
            yield gen.with_timeout(timedelta(seconds=timeout),
                    self.center.unregister(address=(self.ip, self.port)))
        self.center.close_streams()
        self.stop()
        self.executor.shutdown()
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)

        for k, v in self.services.items():
            v.stop()
        self.status = 'closed'
        self.stop()

    @gen.coroutine
    def terminate(self, stream, report=True):
        yield self._close(report=report)
        raise Return('OK')

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

    @property
    def address_tuple(self):
        return (self.ip, self.port)

    @gen.coroutine
    def gather(self, stream=None, who_has=None):
        who_has = {k: [coerce_to_address(addr) for addr in v]
                    for k, v in who_has.items()
                    if k not in self.data}
        try:
            result = yield gather_from_workers(who_has)
        except KeyError as e:
            logger.warn("Could not find data during gather", exc_info=True)
            raise Return({'status': 'missing-data',
                          'keys': e.args})
        else:
            self.data.update(result)
            raise Return({'status': 'OK'})

    @gen.coroutine
    def _ready_task(self, function=None, key=None, args=(), kwargs={},
            task=None, who_has=None):
        if who_has:
            local_data = {k: self.data[k] for k in who_has if k in self.data}
            who_has = {k: set(map(coerce_to_address, v))
                       for k, v in who_has.items()
                       if k not in self.data}
            try:
                logger.debug("gather %d keys from peers: %s",
                            len(who_has), str(who_has))
                start = default_timer()
                other = yield gather_from_workers(who_has)
                transfer_time = default_timer() - start
                data = merge(local_data, other)
            except KeyError as e:
                logger.warn("Could not find data during gather in compute",
                            exc_info=True)
                raise Return({'status': 'missing-data',
                              'keys': e.args,
                              'key': key})
        else:
            data = {}
            transfer_time = 0
        try:
            start = default_timer()
            if task is not None:
                task = loads(task)
            if function is not None:
                function = loads(function)
            if args:
                args = loads(args)
            if kwargs:
                kwargs = loads(kwargs)
            deserialization_time = default_timer() - start
        except Exception as e:
            logger.warn("Could not deserialize task", exc_info=True)
            raise Return(assoc(error_message(e), 'key', key))

        if task is not None:
            assert not function and not args and not kwargs
            function = execute_task
            args = (task,)

        # Fill args with data
        args2 = pack_data(args, data)
        kwargs2 = pack_data(kwargs, data)

        raise Return({'status': 'OK',
                      'function': function,
                      'args': args2,
                      'kwargs': kwargs2,
                      'diagnostics': {'transfer': transfer_time,
                                      'deserialization': deserialization_time},
                      'key': key})

    @gen.coroutine
    def executor_submit(self, key, function, *args, **kwargs):
        """ Safely run function in thread pool executor

        We've run into issues running concurrent.future futures within
        tornado.  Apparently it's advantageous to use timeouts and periodic
        callbacks to ensure things run smoothly.  This can get tricky, so we
        pull it off into an separate method.
        """
        job_counter[0] += 1
        i = job_counter[0]
        logger.debug("Start job %d, %s", i, key)
        future = self.executor.submit(function, *args, **kwargs)
        pc = PeriodicCallback(lambda: logger.debug("future state: %s - %s",
            key, future._state), 1000); pc.start()
        try:
            if sys.version_info < (3, 2):
                yield future
            else:
                while not future.done() and future._state != 'FINISHED':
                    try:
                        yield gen.with_timeout(timedelta(seconds=1), future)
                        break
                    except gen.TimeoutError:
                        logger.debug("work queue size: %d", self.executor._work_queue.qsize())
                        logger.debug("future state: %s", future._state)
                        logger.debug("Pending job %d: %s", i, future)
        finally:
            pc.stop()

        result = future.result()
        logger.debug("Finish job %d, %s", i, key)
        return result

    @gen.coroutine
    def compute_stream(self, stream):
        logger.info("Open compute stream")

        @gen.coroutine
        def process(msg):
            result = yield self.compute(report=False, **msg)
            yield write(stream, result)

        with log_errors():
            while True:
                msg = yield read(stream)
                op = msg.pop('op', None)
                if op == 'close':
                    break
                if op == 'compute-task':
                    process(msg)
                else:
                    logger.warning("Unknown operation %s, %s", op, msg)

        stream.close()
        logger.info("Close compute stream")

    @gen.coroutine
    def compute(self, stream=None, function=None, key=None, args=(), kwargs={},
            task=None, who_has=None, report=True):
        """ Execute function """
        with log_errors():
            self.active.add(key)

            # Ready function for computation
            msg = yield self._ready_task(function=function, key=key, args=args,
                kwargs=kwargs, task=task, who_has=who_has)
            if msg['status'] != 'OK':
                self.active.remove(key)
                raise Return(msg)
            else:
                function = msg['function']
                args = msg['args']
                kwargs = msg['kwargs']

            # Log and compute in separate thread
            result = yield self.executor_submit(key, apply_function, function,
                                                args, kwargs)
            result['key'] = key
            result.update(msg['diagnostics'])

            if result['status'] == 'OK':
                self.data[key] = result.pop('result')
                if report:
                    response = yield self.center.add_keys(address=(self.ip, self.port),
                                                          keys=[key])
                    if not response == 'OK':
                        logger.warn('Could not report results to center: %s',
                                    response.decode())
            else:
                logger.warn(" Compute Failed\n"
                    "Function: %s\n"
                    "args:     %s\n"
                    "kwargs:   %s\n",
                    str(funcname(function))[:1000], str(args)[:1000],
                    str(kwargs)[:1000], exc_info=True)

            logger.debug("Send compute response to scheduler: %s, %s", key, msg)
            with ignoring(KeyError):
                self.active.remove(key)
            raise Return(result)

    @gen.coroutine
    def run(self, stream, function=None, args=(), kwargs={}):
        function = loads(function)
        if args:
            args = loads(args)
        if kwargs:
            kwargs = loads(kwargs)
        try:
            result = function(*args, **kwargs)
        except Exception as e:
            logger.warn(" Run Failed\n"
                "Function: %s\n"
                "args:     %s\n"
                "kwargs:   %s\n",
                str(funcname(function))[:1000], str(args)[:1000],
                str(kwargs)[:1000], exc_info=True)

            response = error_message(e)
        else:
            response = {
                'status': 'OK',
                'result': dumps(result),
            }
        raise Return(response)

    @gen.coroutine
    def update_data(self, stream, data=None, report=True):
        data = valmap(loads, data)
        self.data.update(data)
        if report:
            response = yield self.center.add_keys(address=(self.ip, self.port),
                                                  keys=list(data))
            assert response == 'OK'
        info = {'nbytes': {k: sizeof(v) for k, v in data.items()},
                'status': 'OK'}
        raise Return(info)

    @gen.coroutine
    def delete_data(self, stream, keys=None, report=True):
        for key in keys:
            if key in self.data:
                del self.data[key]
        logger.debug("Deleted %d keys", len(keys))
        if report:
            logger.debug("Reporting loss of keys to center")
            yield self.center.remove_keys(address=self.address,
                                          keys=list(keys))
        raise Return('OK')

    def get_data(self, stream, keys=None):
        return {k: dumps(self.data[k]) for k in keys if k in self.data}

    def upload_file(self, stream, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_dir, filename)
        if isinstance(data, unicode):
            data = data.encode()
        with open(out_filename, 'wb') as f:
            f.write(data)
            f.flush()

        if load:
            try:
                name, ext = os.path.splitext(filename)
                if ext in ('.py', '.pyc'):
                    logger.info("Reload module %s from .py file", name)
                    name = name.split('-')[0]
                    reload(import_module(name))
                if ext == '.egg':
                    sys.path.append(out_filename)
                    pkgs = pkg_resources.find_distributions(out_filename)
                    for pkg in pkgs:
                        logger.info("Load module %s from egg", pkg.project_name)
                        reload(import_module(pkg.project_name))
                    if not pkgs:
                        logger.warning("Found no packages in egg file")
            except Exception as e:
                logger.exception(e)
                return {'status': 'error', 'exception': dumps(e)}
        return {'status': 'OK', 'nbytes': len(data)}


job_counter = [0]


def execute_task(task):
    """ Evaluate a nested task

    >>> inc = lambda x: x + 1
    >>> execute_task((inc, 1))
    2
    >>> execute_task((sum, [1, 2, (inc, 3)]))
    7
    """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    elif isinstance(task, list):
        return list(map(execute_task, task))
    else:
        return task


cache = dict()


def dumps_function(func):
    """ Dump a function to bytes, cache functions """
    if func not in cache:
        b = dumps(func)
        cache[func] = b
    return cache[func]


def dumps_task(task):
    """ Serialize a dask task

    Returns a dict of bytestrings that can each be loaded with ``loads``

    Examples
    --------
    Either returns a task as a function, args, kwargs dict

    >>> from operator import add
    >>> dumps_task((add, 1))  # doctest: +SKIP
    {'function': b'\x80\x04\x95\x00\x8c\t_operator\x94\x8c\x03add\x94\x93\x94.'
     'args': b'\x80\x04\x95\x07\x00\x00\x00K\x01K\x02\x86\x94.'}

    Or as a single task blob if it can't easily decompose the result.  This
    happens either if the task is highly nested, or if it isn't a task at all

    >>> dumps_task(1)  # doctest: +SKIP
    {'task': b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'}
    """
    if istask(task):
        if task[0] is apply and not any(map(_maybe_complex, task[2:])):
            d = {'function': dumps_function(task[1]),
                 'args': dumps(task[2])}
            if len(task) == 4:
                d['kwargs'] = dumps(task[3])
            return d
        elif not any(map(_maybe_complex, task[1:])):
            return {'function': dumps_function(task[0]),
                        'args': dumps(task[1:])}
    return {'task': dumps(task)}


def apply_function(function, args, kwargs):
    """ Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    start = default_timer()
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        msg = error_message(e)
    else:
        msg = {'status': 'OK',
               'result': result,
               'nbytes': sizeof(result),
               'type': dumps_function(type(result)) if result is not None else None}
    finally:
        end = default_timer()
    msg['compute-time'] = end - start
    return msg
