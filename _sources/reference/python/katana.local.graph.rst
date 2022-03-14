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

- The projection will only be functional while the original graph remains in memory. Deleting the original graph while the projected one is still in use may cause erratic behavior with the projection. Make sure to delete any projections using Python’s `del` command before doing the same with the original graph.


.. |supports_compiled_operator| replace::
    This method may be used in compiled operators with some restrictions.

.. |lazy_compute| replace::
    The information required to perform this operation efficiently is computed lazily on the first call to this
    method. This information is shared between related methods when possible.

.. autoclass:: katana.local.Graph
   :special-members: __init__, __iter__, __getitem__, __setitem__, __len__
   :exclude-members: out_edge_ids_for_node, out_edge_ids_for_node_and_type, in_edge_ids_for_node, in_edge_ids_for_node_and_type, out_degree_for_type, in_degree_for_type
