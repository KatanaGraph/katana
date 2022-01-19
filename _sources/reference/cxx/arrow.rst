=====
Arrow
=====

Inspecting Arrow Data
=====================

In addition to the Arrow API, there are some support functions in the Katana
library to help inspect Arrow data in a type-safe way.

.. doxygenfunction:: katana::VisitArrow

.. doxygenclass:: katana::ArrowVisitor

.. doxygenfunction:: katana::GetArrowTypeID(const arrow::Scalar&)

.. doxygenfunction:: katana::GetArrowTypeID(const arrow::Array&)

.. doxygenfunction:: katana::GetArrowTypeID(const arrow::ArrayBuilder*)

.. doxygenfunction:: katana::ArrayFromScalars
