Network Architecture
====================

Dask is a centralized distributed system.  There are three kinds of processes:

1.  A single centralized scheduler
2.  Many workers
3.  Many clients

The scheduler coordinates the workers and takes requests from the clients.  It
maintains a consistent state during this process, responding to stimuli from
the other processes.

The workers get work from the scheduler, process tasks, and then report back
when they have completed.  They hold on to the finished results and communicate
with each other to share data as necessary.

The clients communicate only to the scheduler and are independent from each
other.

Protocol
--------

See Protocol_.

.. _Protocol: http://distributed.readthedocs.io/en/latest/protocol.html

High volume messages
--------------------

Some channels of communication might see very many small messages.  For
performance these are batched over time intervals.  The first message to be put
on a communication stream is sent immediately.  Any further messages sent
within a fixed time interval (such as 2ms) are buffered and sent once the time
interval has passed.  These are all then sent as a list of messages.  This
both reduces network traffic (messages can be much smaller than a packet) and
also reduces serialization overhead.  This helps when clients submit many tasks
one after another or when workers compute those tasks, such as would happen in
the following case:

.. code-block:: python

   futures = []
   for x in data:
       f = e.submit(inc, x)
       futures.append(f)

It *is* faster to send these many small functions as a single call to some
larger function, like ``compute`` or ``map``, but only by about a factor of
two.  The flexibility to abuse ``submit`` in this way without significant cost
provides users a robust fallback in complex situations.
