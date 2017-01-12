from __future__ import print_function, division, absolute_import

from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import timedelta
import logging

from six import with_metaclass

from tornado import gen

from ..metrics import time


logger = logging.getLogger(__name__)

# Connector instances

connectors = {
    #'tcp': ...,
    #'zmq': ...,
    }


# Listener classes

listeners = {
    #'tcp': ...,
    # 'zmq': ...,
    }


DEFAULT_SCHEME = 'tcp'


class CommClosedError(IOError):
    pass


class Comm(with_metaclass(ABCMeta)):
    """
    A message-oriented communication object, representing an established
    communication channel.
    """

    # XXX add set_close_callback()?

    @abstractmethod
    def read(self, deserialize=None):
        """
        Read and return a message.  If *deserialize* is not None, it
        overrides this communication's default setting.

        This method is a coroutine.
        """

    @abstractmethod
    def write(self, msg):
        """
        Write a message (a picklable Python object).

        This method is a coroutine.
        """

    @abstractmethod
    def close(self):
        """
        Close the communication cleanly.

        This method is a coroutine.
        """

    @abstractmethod
    def abort(self):
        """
        Close the communication abruptly.
        """

    @abstractmethod
    def closed(self):
        """
        Return whether the stream is closed.
        """

    @abstractproperty
    def peer_address(self):
        """
        Return the peer's address.  For logging and debugging purposes only.
        """


class Listener(with_metaclass(ABCMeta)):

    @abstractmethod
    def start(self):
        """
        Start listening for incoming connections.
        """

    @abstractmethod
    def stop(self):
        """
        Stop listening.  This does not shutdown already established
        communications, but prevents accepting new ones.
        """
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()

    @abstractproperty
    def address(self):
        """
        The listening address as a URL string
        (e.g. 'tcp://something').
        """
        # XXX the returned address is not always contactable, e.g.
        # 'tcp://0.0.0.0:1234'.  Should we have another property
        # for that?


def parse_address(addr):
    if not isinstance(addr, str):
        raise TypeError("expected str, got %r" % addr.__class__.__name__)
    scheme, sep, loc = addr.rpartition('://')
    if not sep:
        scheme = DEFAULT_SCHEME
    return scheme, loc


def unparse_address(scheme, loc):
    return '%s://%s' % (scheme, loc)


def normalize_address(addr):
    """
    """
    return unparse_address(*parse_address(addr))


@gen.coroutine
def connect(addr, timeout=3, deserialize=True):
    """
    Connect to the given address (a URI such as 'tcp://127.0.0.1:1234')
    and yield a Comm object.
    """
    scheme, loc = parse_address(addr)
    connector = connectors.get(scheme)
    if connector is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    start = time()
    deadline = start + timeout
    while True:
        future = connector.connect(loc, deserialize=deserialize)
        try:
            comm = yield gen.with_timeout(timedelta(seconds=deadline - time()),
                                          future,
                                          quiet_exceptions=EnvironmentError)
        except EnvironmentError:
            if time() < deadline:
                yield gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                raise
        except gen.TimeoutError:
            raise IOError("Timed out while connecting to %r" % (addr,))
        else:
            break

    raise gen.Return(comm)


def listen(addr, handle_comm, deserialize=True):
    """
    Listen on the given address (a URI such as 'tcp://192.168.1.254')
    and call *handle_comm* with a Comm object on each incoming connection.
    """
    scheme, loc = parse_address(addr)
    listener_class = listeners.get(scheme)
    if listener_class is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    return listener_class(loc, handle_comm, deserialize)
