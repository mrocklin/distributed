Task Scheduling
===============

Overview
--------

The life of a computation with Dask can be described in the following stages:

1.  The user authors a graph using some library, perhaps Dask.delayed or
    dask.dataframe or the ``submit/map`` functions on the client.  They submit
    these tasks to the scheduler.
2.  The schedulers assimilates these tasks into its graph of all tasks to track
    and as their dependencies become available it asks workers to run each of
    these tasks.
3.  The worker recieves information about how to run the task, communicates
    with its peer workers to collect dependencies, and then runs the relevant
    function on the appropriate data.  It reports back to the scheduler that it
    has finished.
4.  The scheduler reports back to the user that the task has completed.  If the
    user desires, it then fetches the data from the worker through the
    scheduler.

Scheduling
----------

Most relevant logic is in tracking tasks as they evolve from newly submitted,
to waiting for dependencies, to ready to run, to actively running on some
worker, to finished in memory, to garbage collected.  Tracking this process,
and tracking all effects that this task has on other tasks that might depend on
it, is the majority of the complexity of the dynamic task scheduler.  This
section describes the system used to perform this tracking.

State
~~~~~

We start with a description of the state that the scheduler keeps on each task.
Each of the following is a dictionary keyed by task name (described below):

* **tasks:** ``{key: task}``:

    Dictionary mapping key to a serialized task.

    A key is the name of a task,
    generally formed from the name of the function, followed by a hash of the
    function and arguments, like ``'inc-ab31c010444977004d656610d2d421ec'``.

    The value of this dictionariy is the task, which is an unevaluated function
    and arguments.  This is stored in one of two forms:

    *  ``{'function': inc, 'args': (1,), 'kwargs': {}}``; a dictionary with the
      function, arguments, and keyword arguments (kwargs).  However in the
      scheduler these are stored serialized, as they were sent from the client,
      so it looks more like ``{'function': b'\x80\x04\x95\xcb\...', 'args':
      b'...', }``

    * ``{'task': (inc, 1)}``: a tuple satisfying the dask graph protocol.  This
      again is stored serialized.

    These are the values that will eventually be sent to a worker when the task
    is ready to run.

* **dependencies and dependents:** ``{key: {keys}}``:

   These are dictionaries which show which tasks depend on which others.  They
   contain redundant information.  If ``dependencies[a] == {b, c}`` then the
   task with the name of ``a`` depends on the results of the two tasks with the
   names of ``b`` and ``c``.  There will be complimentary entries in dependents
   such that ``a in dependents[b]`` and ``a in dependents[c]`` such as
   ``dependents[b] == {a, d}``.  Keeping the information around twice allows
   for constant-time access for either direction of query, so we can both look
   up a task's out-edges or in-edges efficiently.

* **waiting and waiting_data:** ``{key: {keys}}``:

   These are dictionaries very similar to dependencies and dependents, but they
   only track keys that are still in play.  For example ``waiting`` looks like
   ``dependencies``, tracking all of the tasks that a certain task requires
   before it can run.  Howver as tasks are completed and arrive in memory they
   are removed from their dependents sets in ``waiting``, so that when a set
   becomes empty we know that a key is ready to run and ready to be allocated
   to a worker.

   The ``waiting_data`` dictionary on the other hand holds all of the
   dependents of a key that have yet to run and still require that this task
   stay in memory in services of tasks that may depend on it (its
   ``dependents``).  When a value set in this dictionary becomes empty its task
   may be garbage collected (unless some client actively desires that this task
   stay in memory).

* **task_state:** ``{key: string}``:

    The ``task_state`` dictionary holds the current state of every key.
    Current valid states include released, waiting, queue, stacks, no-worker,
    processing, memory, and erred.  These states will be explained further in
    TODO_

* **priority:** ``{key: tuple}``:

    The ``priority`` dictionary provides each key with a relative ranking.
    This ranking is generally a tuple of two parts.  The first (and dominant)
    part corresponds to when it was submitted.  Generally ealier tasks take
    precedence.  The second part is determined by the client, and is a way to
    prioritize tasks within a large graph that may be important, such as if
    they are on the critical path, or good to run in order to release many
    dependencies.  This will be explained further in TODO_.

    A key's priority is only used to break ties, when many keys are being
    considered for execution.  The priority does *not* determine running order,
    but does exert some subtle influence that does significantly shape the long
    term performance of the cluster.

* **ready:** ``deque(key)``

    A deque of keys that are ready to run now but haven't yet been sent to a
    worker to run.  These keys show no affinity to any particular worker and so
    can be run equally well by anyone.  This is a common pool of tasks.

* **stacks:** ``{worker: [keys]}``:

    Keys that are ready to run and show some affinity to a particular worker.
    These keys typically have dependencies that are known to be on that worker
    or have user-defined restrictions that require them to run on certain
    nodes.  A worker pulls from this stack first before taking tasks from the
    common pool of ``ready`` tasks.

* **processing:** ``{worker: {key: cost}}``:

    Keys that are currently running on a worker.  This is keyed by worker
    address and contains the expected cost in seconds of running that task.

* **rprocessing:** ``{key: {worker}}``:

    The reverse of the ``processing`` dictionary.  This is all keys that are
    currently running with a set of all workers that are currently running
    them.  This is redundant with ``processing`` and just here for faster
    indexed querying.

* **who_has:** ``{key: {worker}}``:

    For keys that are in memory this shows on which workers they currently
    reside.

* **has_what:** ``{worker: {key}}``:

    This is the transpose of ``who_has``, showing all keys that currently
    reside on each worker.

* **released:** ``{keys}``

    The set of keys that are known, but released from memory.  These have
    typically run to completion and are no longer necessary.

* **unrunnable:** ``{key}``

    The set ``unrunnable`` contains keys that are not currently able to run,
    probably because they have a user defined restriction (described below)
    that is not met by any avaialble worker.  These keys are waiting for an
    appropriate worker to join the network before computing.

* **retrictions:** ``{key: {hostnames}}``:

    A set of hostnames per key of where that key can be run.  Usually this
    is empty unless a key has been specifically restricted to only run on
    certain hosts.  These restrictions don't include a worker port.  Any
    worker on that hostname is deemed valid.

    Restrictions are described further in TODO_

* **loose_retrictions:** ``{key}``:

    Set of keys for which we are allowed to violate restrictions (see above) if
    not valid workers are present and the task would otherwise go into the
    ``unrunnable`` set.

*  **exceptions and tracebacks:** ``{key: Exception/Traceback}``:

    Dictionaries mapping keys to remote exceptions and tracebacks.  When tasks
    fail we store their exceptions and tracebacks (serialized from the worker)
    here so that users may gather the exceptions to see the error.

*  **exceptions_blame:** ``{key: key}``:

    If a task fails then we mark all of its dependent tasks as failed as well.
    This dictionary lets any failed task see which task was the origin of its
    failure.

* **suspicious_tasks:** ``{key: int}``

    Number of times a task has been involved in a worker failure.  Some tasks
    may cause workers to fail (such as ``sys.exit(0)``).  When a worker fails
    all of the tasks on that worker are reassigned to others.  This combination
    of behaviors can cause a bad task to catastrophically destroy all workers
    on the cluster, one after another.  Whenever a worker fails we mark each
    task currently running on that worker as suspicious.  If a worker is
    involved in three failures (or some other fixed constant) then we mark the
    task as failed.

* **who_wants:** ``{key: {client}}``:

    When a client submits a graph to the scheduler it also specifies which
    output keys it desires.  Those keys are tracked here where each desired key
    knows which clients want it.  These keys will not be released from memory
    and, when they complete, messages will be sent to all of these clients that
    the task is ready.

* **wants_what:** ``{client: {key}}``:

    The transpose of ``who_wants``.

* **nbytes:** ``{key: int}``:

    The number of bytes, as determined by ``sizeof``, of the result of each
    finished task.  This number is used for diagnostics and to help prioritize
    work.

* **stealable:** ``[[key]]``

    A list of stacks of stealable keys, ordered by stealability.  For more
    information see TODO_


Stimuli
-------

Whenever an event happens, like when a client sends up more tasks, or when a
worker finishes a task, the scheduler changes the state above.  For example
when a worker reports that a task has finished we perform actions like the
following:

**Task ``key`` finished by ``worker**:

.. code-block:: python

   task_state[key] = 'memory'

   who_has[key].add(worker)
   has_what[worker].add(key)

   nbytes[key] = nbytes

   processing[worker].remove(key)
   del rprocessing[key]

   if key in who_wants:
       send_done_message_to_clients(who_wants[key])

   for dep in dependencies[key]:
      waiting_data[dep].remove(key)

   for dep in dependents[key]:
      waiting[dep].remove(key)

   if stacks[worker]:
       next_task = stacks[worker].pop()
   elif ready:
       next_task = ready.pop()
   else:
       idle.add(worker)
