======
Graphs
======

This page lists all of the available properties and functions for a ``Graph``.
A ``Graph`` is created by calling one of the import routines from
:py:mod:`~katana.local.import_data`.

You can also create a ``Graph`` by calling the :py:meth:`~katana.local.Graph.project` method from a ``Graph``. A graph projection is an operation that filters an existing graph's nodes, edges, or both based on their types and then creates a new graph. You can then perform analytics with the new graph.

.. comment: 2022-03-07 eventually we'll be able to filter on more than just types

When working with graph projections, keep in mind that:

- The resulting `Graph` projection object shares property data with the original graph. If a user runs an analytics routine such as PageRank on the projection, this will add a new node property to both the projection and the original graph.

- The projection will only be functional while the original graph remains in memory. Deleting the original graph while the projected one is still in use may cause erratic behavior with the projection. 
  
.. autoclass:: katana.local.Graph
   :special-members: __init__, __iter__, __getitem__, __setitem__, __len__
