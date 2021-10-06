===========
Katana APIs
===========

.. _Three-Katana-APIs:

The Three Katana APIs
=====================

Katana provides three distinct Python APIs. All APIs provide access to graph
data and graph routines. However, they provide it from different contexts and
data placement is different. Two of the APIs provide high-performance parallel
looping constructs that allow writing custom graph algorithms.

.. list-table::
   :header-rows: 1

   * -
     - Called from
     - Data placement
     - Distributed
     - Graph scale
   * - `The Local API`_
     - :ref:`Client <Client-Environment>`, :ref:`Cluster <Cluster-Environment>`, :ref:`Local <Local-environment>`
     - Calling process
     - No
     - Small
   * - `The Distributed API`_
     - :ref:`Cluster <Cluster-Environment>`
     - Cluster
     - Directly
     - Large
   * - `The Remote API`_
     - :ref:`Client <Client-Environment>`
     - Cluster
     - Indirectly by remote execution
     - Large

Katana Local API
----------------

The local or single-host API, :py:mod:`katana.local`, provides graph data
access, local graph loading, and shared-memory analytics. This API supports
writing new graph algorithms using high-performance parallel loops. This API
does not require or utilize a remote server (though it can be used along with
the other APIs which do use remote servers and clusters). The target audience of
``katana.local`` is people who want to process or analyze smaller graphs (that
fit in the memory of a single computer) either as a way to test drive Katana or
as part of a real data science pipeline that does not require scale out.

Katana Remote API
-----------------

The :py:mod:`katana.remote` API provides remote access to distributed analytics,
launching distributed algorithms on the cluster, and graph management. The API
also provides data ingest into the cluster, and convenient, but low-performance,
access to graph data. This API *does not* support writing new graph algorithms.

The target audience of ``katana.remote`` is all users of Katana enterprise.
People would solely use ``katana.remote`` if they want a high-level and easy to
use interface to out-of-the-box features provided by Katana, but do not require
custom algorithms or new integrations. People who need more control or features
will use `the distributed API`_ along with ``katana.remote``.  ``katana.remote``
supports launching functions which use ``katana.distributed`` on the Katana
cluster.

Katana Distributed API
----------------------

The distributed API, :py:mod:`katana.distributed`, provides access to graph data
and graph processing on the Katana computing cluster. This provides higher
performance, but also requires much more expertise to use correctly. The
distributed API provides access the local graph partition (the data placed at
the current host) and information about the partitioning of the global graph
over the hosts in the cluster. This API supports writing new distributed graph
algorithms using high-performance parallel loops and distributed synchronization
provided by :py:class:`katana_enterprise.distributed.GluonSubstrate`.  This API
will generally be used along with the parallelism tools provided in
:py:mod:`katana` and :py:mod:`katana.local` (especially parallel loops).

The target audience of ``katana.distributed`` is experienced programmers who
want to write their own algorithms or squeeze out the last bit of performance
when calling several routines on a small graph (when call overhead would be an
issue). The distributed interface may also be needed to integrate Katana into
third-party systems (e.g., graph AI systems).

Execution Environments
======================

The APIs discussed in :ref:`above <Three-Katana-APIs>` are used in combination
in three different environments each supporting a different combination of APIs.

.. _Client-Environment:

Client Environment
------------------

This environment is provided by Katana as a container image or installable
package set and can execute on a user’s local machine. It provides a Jupyter
environment for programming. The environment provides `the local API`_, as well
as `the remote API`_. This environment allows a user to manipulate and analyze
graphs in Katana’s distributed storage engine via the Katana server. The user
can run routines on graphs and get results and perform local operations on
smaller graphs as needed. Programs running in this environment are generally
sequential and have no implicit or specialized concurrency or parallelism (the
user can use standard Python features for parallelism or concurrency if they
want).  Per-call graph access overhead from this environment is extremely high
due to the remote call. For fast access to graph data or to implement new
algorithms, the user can dispatch algorithms to execute on the cluster in the
`cluster environment`_.

.. _Cluster-Environment:

Cluster Environment
-------------------

This cluster environment exists inside the Katana cluster. This environment
provides `the local API`_ and `the distributed API`_. This environment executes
within MPI and inherits its execution model (Single Program Multiple Data;
SPMD). All programs running in this environment must be written with this model
in mind. The programs are implicitly parallel due to how they are executed and
can also include explicit parallelism in the form of Katana parallel loops.
Non-Katana parallelism is unsafe and unsupported. The user can write new
high-performance algorithms to execute in this environment.

.. _Local-Environment:

Local-only Environment
----------------------

This is the environment provided by the open-source edition of Katana. The only
API available is `the local API`_.  This environment allows a user to process
and analyze small graphs (up to 10s of GBs), including writing new algorithms,
but does not support large scale, distributed execution. Programs running in
this environment are sequential other than within the Katana parallel loops.

Katana Library
==============

.. automodule:: katana
   :members:
   :undoc-members:
   :special-members: __init__, __iter__, __getitem__, __setitem__, __len__
