Related Work
============

Dask is often compared both to task schedulers like Airflow and Ciel and to
"big data" frameworks like Spark and Elemental.

Task Schedulers
---------------

The context of task scheduling is described in :doc:`Background
Scheduling<background-scheduling>`.

Industry
~~~~~~~~

Pragmatic data engineering projects commonly found in industry within this space include systems like Make, Airflow_, and Luigi.

Make is arguably the best known task scheduler, known by many software
developers tasked with compiling a complex codebase.  It defines tasks as shell
commands and data as compilation artifacts.  The dependency structure is
clearly defined in the Make language.  The system is good at identifying novel
changes and recovering smoothly without performing unnecessary work.

Airflow could be very loosely defined as a combination of Make with cron, but
smoother and with policy options for modern workflows.  Airflow allows jobs to
fire off at particular time intervals like "download this data every midnight"
and attaches dependencies like "once that data is downloaded, clean it with
this script."  It also provides policies for what to do if things fail,
assiging jobs to particular machines, etc..  The policies of Airflow are
interesting to observe because they give a good view into what is required by a
task scheduler in the wild.

.. _Make: https://en.wikipedia.org/wiki/Make_(software)
.. _Airflow: https://github.com/apache/incubator-airflow
.. _Luigi: http://luigi.readthedocs.io/en/latest/

In comparison Dask is more computationally minded than these projects.  It
focuses on millisecond response times and cares strongly about stateful data on
workers and efficient data transfer between workers.  It does not have a rich
system to describe cron-like or other policies useful in data engineering.
This logic would have to be built around Dask.


Academic
~~~~~~~~

There is a wealth of literature on static scheduling and on task scheduling
without dependencies.  We skip these entirely.

High performance computing has produced some quality task schedulers that
operate in different regimes.

Swift-lang_ has typically scheduled processes that depend on each other in a
complex DAG, such as often occur when running large complex pipelines in
simulations or data analysis.  Swift-T is a newer low-latency system capable of
scheduling many thousandas of tasks per second.

Numerical linear algebra systems, like Plasma_ use dynamic task schedulers,
like the custom built DAGuE_ internally.  This is an interesting case because
the performance constraints of numerical linear algebra are significant, with
very many very short tasks.

Ciel_ is a dynamic task scheduler much in the same point in design space as
Dask.

.. _Ciel: https://www.usenix.org/legacy/events/nsdi11/tech/nsdi11_proceedings.pdf#page=123


Job Schedulers
~~~~~~~~~~~~~~

Traditional job schedulers like SGE, SLURM, Torque, Condor, and LSF provide a
mechanism for users to launch processes on a cluster of computers.  They
provide some ability to describe scripts of simple dependencies between jobs.
These are typically used to deploy large batches of computational processes
to process embarrassingly paralell workloads, or to set up distributed
computing frameworks such as MPI.

More recent systems, like Mesos_, provide similar capabilities, the ability to
launch a distributed framework on a cluster, but operate in a more on-line
fashion, with the framework continuously accepting and releasing resources in a
more fine-grained way.

The internals of these schedulers do not concern us here except in so far as
they are a mechanism to deploy Dask on existing clusters.

.. _Mesos: http://mesos.apache.org/


Big Data Systems
----------------

Through the use of its higher-level interfaces, Dask.array, Dask.dataframe, and
Dask.bag, Dask is able to support some traditional "Big Data" workloads such
as the following:

*  Distributed databases like Impala
*  Map-Shuffle-Reduce paradigms like Hadoop_, Spark_, or historically DryadLINQ_.
*  Parallel Arrays like SciDB or Elemental

Dask differs from these projects in that they typically contain high-level
optimizations for their particular domain.  The query optimization in a well
designed database can reduce the cost of complex queries by orders of
magnitude.  Attention to algorithmic and mathematical details in a system like
Elemental are strictly required to make certain optimization problems
tractible.  The infrastructure within Hadoop's shuffle makes it still the most
scalable game in town, even when compared to newer alternatives.

Dask can mimic some of these optimizations at a low-level.  For example
the low-level compiler optimizations of operator fusion, pushing down
commutative selection and projection operations, slice fusion, etc. can be
implemented in a fine-grained way through linear-time optimization passes on
the task graph.  However other optimizations, such as sophisticated query plans
or the correct choice of mathematical algorithms are beyond the scope for Dask
and in a highly specialized domain the specialized tool will always out-perform
the general tool.

.. _Elemental: http://libelemental.org/
.. _Spark: https://spark.apache.org/
.. _DryadLINQ: https://www.usenix.org/legacy/event/osdi08/tech/full_papers/yu_y/yu_y.pdf
.. _Hadoop: http://hadoop.apache.org/


Task Schedulers within these systems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many of these specialized systems also contain rudimentary task schedulers
within them.  These systems take some high-level representation from the user
like a SQL query or a Spark RDD, optimize it with a query optimizer, and then
emit a task graph in stages.  Performance optimization for these systems is
typically bounded by high level query optimization rather than low-level task
graph optimization for which fairly simple schemes are often used.

Because Dask strives to support general custom computations it can not rely on
high-level query optimizers and instead focuses its efforts on efficient
low-level task graph optimization.


.. _Swift-lang: http://swift-lang.org/main/
.. _DAGuE: http://icl.cs.utk.edu/dague/overview/
.. _Plasma: http://icl.cs.utk.edu/plasma/overview/index.html
