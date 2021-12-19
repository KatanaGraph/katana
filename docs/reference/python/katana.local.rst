================
Katana Local API
================

The local or single-host API, :py:mod:`katana.local`, provides graph data
access, local graph loading, and shared-memory analytics. This API supports
writing new graph algorithms using high-performance parallel loops. This API
does not require or utilize a remote server (though it can be used along with
the other APIs which do use remote servers and clusters). The target audience of
``katana.local`` is people who want to process or analyze smaller graphs (that
fit in the memory of a single computer) either as a way to test drive Katana or
as part of a real data science pipeline that does not require scale out.

.. toctree::
   :maxdepth: 1

   katana.local.graph
   katana.local.analytics
   katana.local.import_data
   katana.local.atomic
   katana.local.datastructures
