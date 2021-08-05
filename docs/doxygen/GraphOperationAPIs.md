# Exposing Graph Operations to Users

**Author:** Arthur Peters (arthurp, amp)

**Note:** Due to doxygen processing things weirdly, this file has a backslash before every at-sign. This is visible on github, but not in the doxygen output. So whatever is shown here "\@" should be read as a single bare at-sign.

Operations are exposed in three steps:

1. [Determine the arguments the operation needs](#Operation-Arguments) and the properties of those arguments,
2. [Define a C++ API for the operation](#c-api) using a set of conventions defined below,
3. [Wrap that API using Cython](#Cython-wrapper-for-Python) to expose the operation to Python.

The API uses a plan to represent the choice of algorithm and any parameters for that algorithm.

No part of the API uses templates because those make it difficult to access from Python.

## Operation Arguments

The operation arguments are split into two categories:

**Plan arguments** which do not affect the result (beyond potentially returning a different valid result in the case of non-deterministic algorithms).
For example, the tile size of an algorithm.
These arguments will affect performance and are included in the plan.

**Functional arguments** which specify the requested result.
For example, the starting node for SSSP.
Many operations will accept an output property name for the output data, and/or input property names to use in the algorithm (e.g., edge weights).
These names should be passed as a `const std::string&`.
These arguments affect output values and are passed as normal argument to the operation.

## C++ API

The C++ API for operations contains 4 components:

1. A `Plan` class that specifies the algorithm and any plan arguments associated with it.
2. The operation function itself, taking a `PropertyGraph*`, functional arguments, and a `Plan`.
3. Optionally, an `AssertValid`  function, taking a `PropertyGraph*`, functional arguments, but no `Plan`.
4. Optionally, a `Statistics` struct that contains any notable graph wide statistics that can be computed from the result of the operation. It has a static `Compute` method taking a `PropertyGraph*`, functional arguments, but no `Plan`.

All functions return `katana::Result` objects to handle errors.
In general, the operation will not return any value itself and so will have return type `katana::Results<void>`.

The operation header and implementation files should be placed as you would other C++ files in the appropriate library.
If the operation is going in `libgalois` the operations are placed in subdirectories.
Look at `libgalois/{src,include}/katana/analytics` for examples.

### Plan

The Plan class should contain any enums or constants used in the plan arguments as public static members.
It should have a zero-argument constructor which creates a reasonable default plan.
Other constructors may exists to automatically create plans based on other information; for instance, a constructor may take a graph and make algorithm choices based on properties of that graph.
Any plan arguments should be stored in private fields of the plan, and have public const accessors.

The Plan class should contain static constructor methods for each general algorithm.
The methods should accept any plan arguments associated with that algorithm.
All the arguments should have reasonable default values.
The argument defaults should be public static constants on the class.
"Automatic" plans (which make algorithmic choices at operation execution time) should not have a static constructor method and instead be created using the class constructor.

The Plan class should subclass `Plan`.
To allow for easier future expansion to GPU and distributed execution, the base class `Plan` has a plan argument `architecture` which should be set to `kCPU` for the moment.

The outline of the C++ plan is as follows:

```c++
class [Operation]Plan : public Plan {
public:
  enum Algorithm {
    [Algo1],
    [...]
  };

  static const int kDefault[plan argument] = [...];
  [...]

private:
  Algorithm algorithm_;
  [... Other plan arguments ...]

  [Operation]Plan(
      Architecture architecture, Algorithm algorithm, ...)
      : Plan(architecture),
        algorithm_(algorithm),
        [...]
        {}

public:
  [Operation]Plan() : [Operation]Plan{kCPU, [default algo], kDefault[arg], ...} {}

  [Operation]Plan(const katana::PropertyGraph* pg) : Plan(kCPU) {
    ...
    if ([properties]) {
      *this = [Algo1]();
    } else {
      *this = [Algo2]();
    }
  }

  Algorithm algorithm() const { return algorithm_; }
  [...]

  static [Operation]Plan [Algo1](
      unsigned arg = kDefault[arg], [...]) {
    return {kCPU, k[Algo1], arg, [...]};
  }
  [...]
};
```

### Operation Function

The operation function takes a graph, any functional arguments, and a plan.
It should return `katana::Results<void>`.
If there are graph wide results (such as, the triangle count), the function may replace `void` with an appropriate value.

The operation should not modify the graph except to add the output properties and to add/remove ephemeral properties used during the algorithm.
If the algorithm must change the graph (for instance, to sort nodes or edges), it should copy the graph before making such changes and then copy any output property back to the original graph (with appropriate transformation) before it returns.

```c++
KATANA_EXPORT Result<void> [Operation](
    PropertyGraph* pg, [... functional arguments ...], [Operation]Plan plan = {});
```

### Validity checking function

The `AssertValid` function should perform a fast check to see if the output of the operation is as expected.
It will generally have false positives (cases where it could return no error when the data is in fact wrong).

It generally takes the same arguments as the operation except for the plan.
It can omit some functional arguments if they can be inferred from the data being checked.
It returns `katana::ErrorCode::AssertionFailed` if the output is not as expected.

```c++
KATANA_EXPORT Result<void> [Operation]AssertValid(
    PropertyGraph* pg, [... functional arguments ...]);
```

This function can be omitted if there is no good way to check the results of the algorithm quickly.

### Statistics

The Statistics struct analyzes the output of the operation and produces useful statistics about the graph as a whole.
The struct can `Print` itself (by default to `stdout` for debugging).
The values are accessible directly as fields.
Useful values computed from the fields can be included using small computed accessor methods.

The struct is created using a `Compute` static method on the struct.
It generally takes the same arguments as the operation except for the plan.
It can omit some functional arguments if they can be inferred from the data being checked.

```c++
struct KATANA_EXPORT [Operation]Statistics {
  [... statistics ...]

  [... computed statistics methods ...]

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<[Operation]Statistics> Compute(
      PropertyGraph* pg, [... functional arguments ...]);
};
```

### Documentation

The following elements should be documented, but you can document other things of course.

1. Every constructor and static method constructor on the Plan.
2. The operation function.
3. Every member of the Statistics struct.

### Complete Example: SSSP

See [libgalois/include/katana/analytics/sssp/sssp.h](../libgalois/include/katana/analytics/sssp/sssp.h).

## Cython wrapper for Python

We wrap the above API using Cython to expose it to Python.
The Cython code has two sections: A description of the C++ API in Cython, and Python functions and classes written in Cython using that API.

The Python wrapper for your operation will go in the Python package (in either the open or enterprise).
The operation should appear in a subpackage of `katana`.
The analytics appear in `katana.local.analytics`, and other categories of operation should consider the same convention.
Though if there is a reason to place them elsewhere that is fine.

*Note:*
Python packages are often deeply nested and this does not have the problems associated with nested C++ namespaces.
Python does not implicitly make enclosing package's contents visible.

### C++ API Description

The description directly parallels the C++ header file (discussed above).
It provides the Cython compiler the information it needs to generate C++ code that accesses the API.

To avoid name conflicts, the C++ classes are imported with a leading `_`.
The Python classes have the name with no initial `_`.
Functions do not need this treatment because the python versions are `snake_case` and do not conflict with the `CamelCase` C++ functions.

Many C++ types will need to be imported from the Cython `libc` and `libcpp` modules with statements like:
`from libcpp.string cimport string` (note **c**import not just import).
It may be useful to copy the imports from the [example](#Complete-Example:-SSSP-Cython).
Make sure to remove any imports you do not actually use.

```cython
cdef extern from "[operation header]" namespace "[namespace]" nogil:
    cppclass _[Operation]Plan "[namespace]::[Operation]Plan" (_Plan):
        enum Algorithm:
            k[Algo] "[namespace]::[Operation]Plan::k[Algo]"
            [...]

        _[Operation]Plan()
        _[Operation]Plan(const _PropertyGraph * pg)

        _[Operation]Plan.Algorithm algorithm() const
        [... Other plan arguments ...]

        \@staticmethod
        _[Operation]Plan [Algo]([...])
        [...]

    [type] kDefault[Arg] "[namespace]::[Operation]Plan::kDefault[Arg]"
    [...]

    Result[void] [Operation](_PropertyGraph* pg, [...], _[Operation]Plan plan)

    Result[void] [Operation]AssertValid(_PropertyGraph* pg, [...])

    cppclass _[Operation]Statistics  "[namespace]::[Operation]Statistics":
        [type] [statistic_name]
        [...]

        void Print(ostream os)

        \@staticmethod
        Result[_[Operation]Statistics] Compute(_PropertyGraph* pg, [...])
```

### Wrap the Plan

First, we wrap any enums in the plan.
Most plans will have an `Algorithm` enum, but the process is the same for any enum.
Each enum is declared as a Python class derived from `Enum`.
Each enum value is a member which is assigned the same value as the C++ enum.
The Python enum must be declared at the top level because Cython does not support nested classes.
Prefix it with `_` to mark it as internal.
We will "export" it as a member of the Python Plan class below.

```cython
class _[Operation]Algorithm(Enum):
    [Algo1] = _[Operation]Plan.Algorithm.k[Algo1]
    [...]
```

The Plan class is an extension class (`cdef class`) that contains a C++ value and methods that are compiled to native code.
The class derives from a Cython base class `Plan` that parallels the C++ `Plan` class.
It has a native member `underlying_` which is a C++ plan and a method `underlying` that returns a pointer to the C++ base class (called `_Plan` here to avoid name conflicts).
We also have a class member `Algorithm` that is set to the actual algorithm Enum class (yes, classes are just value in Python so you can assign them to members).

The operation's Plan class has the following methods:

* A utility method `make` which constructs a Python Plan from a C++ Plan. This can be copied directly into your code, only providing the type name.
* A Python property (`\@property`) for each plan argument. These may need to convert the C++ value to a Python form.
* A constructor (`__init__`) handling all constructors of the C++ Plan. This may require conditionally calling different C++ constructors. Default arguments do not require overloading though, the default values are just provided to Python as well (this is why we make public constants for the defaults). The arguments may need to be converted from Python to C++.
* A static constructor method for each such method in the C++ Plan. These will be similar to `__init__`.

```cython
cdef class [Operation]Plan(Plan):
    cdef _[Operation]Plan underlying_

    cdef _Plan* underlying(self) except NULL:
        return &self.underlying_

    Algorithm = _[Operation]Algorithm

    \@staticmethod
    cdef [Operation]Plan make(_[Operation]Plan u):
        f = <[Operation]Plan>[Operation]Plan.__new__([Operation]Plan)
        f.underlying_ = u
        return f

    \@property
    def algorithm(self):
        return _[Operation]Algorithm(self.underlying_.algorithm())
    \@property
    def [plan argument](self):
        return self.underlying_.[plan argument]()
    [...]

    def __init__(self, graph = None):
        if graph is None:
            self.underlying_ = _[Operation]Plan()
        else:
            if not isinstance(graph, PropertyGraph):
                raise TypeError(graph)
            self.underlying_ = _[Operation]Plan((<PropertyGraph>graph).underlying.get())

    \@staticmethod
    def delta_tile(unsigned [plan argument] = kDefault[plan argument], [...]) -> [Operation]Plan:
        return [Operation]Plan.make(_[Operation]Plan.DeltaTile([plan argument], [...]))
    [...]

```

### Functions

There are two top-level functions per operation: the operation itself, and the validity check function.
The operation takes a Python `PropertyGraph` (which is a wrapper around `_PropertyGraph`), the functional arguments, and a plan.
The plan should have a default value being the default plan (the plan from the zero argument constructor).
The function will unwrap the `PropertyGraph` and plan, and perform any conversions required for functional arguments.
The function should use `with nogil:` in Cython to allow unlock the Global Interpreter Lock to allow the Python program to do parallel work.
The validity check function is the same as the operation except that it does not take a plan.

Functions in Python are `snake_case`.
If your operation returns a value then you will make need to create a `handle_result_` function similar to that in Statistics below.

```cython
def [operation](PropertyGraph pg, [functional arguments],
         [Operation]Plan plan = [Operation]Plan()):
    with nogil:
        handle_result_void([Operation](pg.underlying.get(), [functional arguments], plan.underlying_))

def [operation]_assert_valid(PropertyGraph pg, [functional arguments]):
    with nogil:
        handle_result_assert([Operation]AssertValid(pg.underlying.get(), [functional arguments]))
```

### Statistics

Due to limitations of Cython we need to create a wrapper function that converts C++ error codes into exceptions.
In this case we call it `handle_result_[Operation]Statistics`.

```cython
cdef _[Operation]Statistics handle_result_[Operation]Statistics(Result[_[Operation]Statistics] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return res.value()
```

The Statistics class is wrapped similarly to the plan.
However, `__init__` creates the underlying object with `Compute` and converts errors into exceptions.
(`Compute` in C++ could be a constructor except for error handling.)
The `Print` method is wrapped into the `__str__` method in Python, to match Python conventions for printing objects.
Statistics accessors are much the same as for the plan, except that the underlying values are fields instead of methods.

```cython
cdef class [Operation]Statistics:
    cdef _[Operation]Statistics underlying

    def __init__(self, PropertyGraph pg, [functional arguments]):
        with nogil:
            self.underlying = handle_result_[Operation]Statistics(_[Operation]Statistics.Compute(
                pg.underlying.get(), [functional arguments]))

    \@property
    def [statistic](self):
        return self.underlying.[statistic]
    [...]

    def __str__(self) -> str:
        cdef ostringstream ss
        self.underlying.Print(ss)
        return str(ss.str(), "ascii")
```

### Complete Example: SSSP Cython

See [python/katana/local/analytics/_sssp.pyx](../python/katana/local/analytics/_sssp.pyx).

### Building the Cython code

Your operation should be in a file called `_[operation].pyx` in the appropriate directory.
In that directory's `CMakeLists.txt` add:

```cmake
add_cython_module(_[operation] _[operation].pyx
    DEPENDS plan
    LIBRARIES [libraries])
```

`[libraries]` should be a list of native libraries required by the operation.
For the analytics operations in the open repository this will be `Katana::galois`.
Other operations may require additional libraries.

The `DEPENDS` line lists other Cython modules that the operation depends on.
All operations will depend on `plan`, but some may also depend on other modules
(e.g., a Cython wrapper for some data structure used in the operation's API).

### Exporting in Python

To provide easy access to your operation, you should add it to a higher-level module.
For instance, `katana.local.analytics` in `katana/analytics/__init__.py`.

Add the following to the appropriate module:

```python
from [package containing the operation]._[operation] import [operation], [operation]_assert_valid, [Operation]Plan, [Operation]Statistics
```

You can export other values or types the same way if needed.
