Dask: A Computational Task Scheduler
====================================

Abstract
--------

Dask is a dynamic distributed task scheduler with data dependencies operating
with sub-millisecond overheads and scaling out to a few thousand nodes.  It is
comprised of a single central scheduler, a number of worker processes, and a
number of client processes sharing a distributed memory space.

Dask supports array and tabular computations, as well as a wide variety of
custom workloads. however it has not been optimized for any of these, instead
performing all optimizations and logic at a low task-based level.  This loss of
a query planner makes it applicable for standard database loads but does make
it highly useful for ad-hoc or custom workloads, such as occur in algorithm
development, data engineering, or atypical computations.

This paper describes the architecture, policies, and optimizations that the
Dask distributed task scheduler uses to administer the computation of large and
evolving task graphs.  It does not go into the specific algorithms of
dask.array or dask.dataframe, except in so far as they are used as examples.


How to read this document
-------------------------

This document assumes some familiarity with the `Dask.distributed API`_ and
occasionally dask.delayed_.  It also uses the Python language (in which Dask
is written).  We encourage you to become loosely familiar with those
interfaces.

.. _`Dask.distributed API`: http://distributed.readthedocs.io/en/latest/api.html
.. _dask.delayed: http://dask.pydata.org/en/latest/delayed.html

This is somewhere between a technical report and online documentation.
References will more often be to web pages than to academic papers.

It contains errors, but is a living document, so feedback is welcome and can be
rapidly assimilated.
