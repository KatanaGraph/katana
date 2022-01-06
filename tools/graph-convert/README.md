Overview
========

Tools provides useful functions for converting graph forms.
`graph-properties-convert` is used for converting property
graphs into *katana form*.

GraphML
=======

GraphML is an xml format for representing graphs and property graphs
A good overview of GraphML can be found at https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjZhs6VyOHqAhVICKwKHeFnDfcQFjAFegQIBhAB&url=http%3A%2F%2Fcs.brown.edu%2Fpeople%2Frtamassi%2Fgdhandbook%2Fchapters%2Fgraphml.pdf&usg=AOvVaw2F0RI4v5jWb4auIb5eZEh2

An example GraphML file is available at external/test-datasets/misc_datasets/array_test.graphml

For practical use of GraphML with the converter:

 - Ensure all property keys are declared before the <graph> tag
 - Ensure all nodes and edges are declared inside the <graph> tag
 - Ensure all nodes appear before any edge
 - Ensure that all instances of a property have the same type (i.e. all ints or all doubles)

Supported types for GraphML:

 - int64_t: attr.type="long"
 - int32_t: attr.type="int"
 - double: attr.type="double"
 - float: attr.type="float"
 - bool: attr.type="boolean"
 - string: attr.type="string"
 - lists:
   - attr.list="long"
   - attr.list="int"
   - attr.list="double"
   - attr.list="float"
   - attr.list="boolean"
   - attr.list="string"

Example GraphML File:

```
<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<key id="name" for="node" attr.name="name"/>
<key id="released" for="node" attr.name="released" attr.type="long"/>
<key id="roles" for="edge" attr.name="roles" attr.type="string" attr.list="string"/>
<graph id="G" edgedefault="directed">
  <node id="n0" labels=":Movie"><data key="released">1999</data></node>
  <node id="n1" labels=":Person"><data key="name">Keanu Reeves</data></node>
  <edge id="e0" source="n1" target="n0" label="ACTED_IN"><data key="roles">["Morpheus","some stuff","test\nn"]</data></edge>
</graph>
</graphml>
```
