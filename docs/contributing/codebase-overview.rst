==============================
Quick Overview of the Codebase
==============================

The code is organized as several C++ and Python libraries. Important C++ libraries
are as follows: 

libsupport
  Contains common utility APIs and configuration headers. Serial code with no runtime state

libgalois
  Contains the parallel runtime and parallel programming interfaces. Parallel
  runtime consists of a thread pool and concurrent allocators. Parallel programming
  interfaces consist of parallel loop constructs and concurrent data structures. It
  depends only on ``libsupport``

libtsuba
  Contains storage layer APIs, including the RDG abstraction. It depends on
  ``libgalois``.

libgraph
  Contains APIs and classes that create the in-memory representation of Graphs,
  and, various Graph Analytics routines. It depends on ``libtsuba``.

lonestar
  Contains command line invokable analytics programs. It depends on ``libgraph``

tools
  Contains graph file format conversion and inspection tools. It depends on
  ``libgraph``
  

.. TODO(amber/amp): Add documentation for python layer

  

