# Stryke

A C++ template library build on top of Orc C++ library. There is also a [python binding here](python). All code is available on [github](https://github.com/edmBernard/Stryke). It's a template "header-only" library but depend on [Apache-Orc](https://orc.apache.org/). All headers are in `include` folder.

Apache-Orc is defined as :
> the smallest, fastest columnar storage for Hadoop workloads.

The C++ Orc lib is highly dynamic. Data Schema is defined at runtime. As Orc have to be bind by dynamic language like python it's perfect. The goal of this lib is to make a more static/templated C++ lib. This code will try to make an easy to use writer and try to fix most of the structure at compilation time.

We currently only support for the following type for data :

| Type | Stryke | Status |
|--|--|--|
|`orc::BYTE`||Not Implemented|
|`orc::INT`|`stryke::Int`||
|`orc::SHORT`|`stryke::Short`||
|`orc::LONG`|`stryke::Long`||
|`orc::STRING`|`stryke::String`||
|`orc::CHAR`||Not Implemented|
|`orc::VARCHAR`||Not Implemented|
|`orc::BINARY`||Not Implemented|
|`orc::FLOAT`|`stryke::Float`||
|`orc::DOUBLE`|`stryke::Double`||
|`orc::DECIMAL`||Not Implemented|
|`orc::BOOLEAN`|`stryke::Boolean`||
|`orc::DATE`|`stryke::Date`||
|`orc::TIMESTAMP`|`stryke::TimeStamp`||
|`orc::STRUCT`||Not Implemented|
|`orc::LIST`||Not Implemented|
|`orc::MAP`||Not Implemented|
|`orc::UNION`||Not Implemented|

We add Two custom type for date that can be initialized by double/long instead of string

| Type | Stryke | Status |
|--|--|--|
|`orc::DATE`|`stryke::DateNumber`||
|`orc::TIMESTAMP`|`stryke::TimeStampNumber`||


## Known limitations/Bugs

* As Stryke is a template Library we have to defined Writer template specialization at compilation. This imply we can't define the schema at runtime (ex: in program argument). If you want these functionnalities the regular C++ Orc lib would be more suitable.

* `close_async` method of `OrcWriterThread`: I made the choice that `close_async` wait the queue to be empty before closing file. If we continue to write data fast enough the file can never close.

* It seem that orc was not able to store timestamps with -1 second "1969-12-31 23:59:59.001" will be saved as "1970-01-01 00:00:00.001"


## Examples

There several writer implemented :
- `OrcWriterImpl` : The basic writer the core implementation
- `OrcWriterDispatch` : It add capability to encode data in folder (ex: `col1=1/col2=2/test-1-2.orc`) for predicate push down in spark. (*Note*: All file are keep open until the writer destruction)
- `OrcWriterSequential` : Define a main key that force to close all open file when it changes.
- `OrcWriterDispatchDuplicate` : Define a main key that appear in folder and in file.
- `OrcWriterSequentialDuplicate` : Define a main key that force to close all open file when it changes. This key is in folder and in file.
- `OrcWriterThread` : Multi threaded writer that can work as `OrcWriterDispatchDuplicate` or `OrcWriterSequentialDuplicate`.

*Note*: All following example assume `using namespace stryke;` is defined.

### Simple writer

```cpp
OrcWriterImpl<Date, Int> writer({"date", "value"}, "example.orc", WriterOptions());
writer.write("2018-12-10", 42);
```

resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents):
```python
{"date": "2018-12-09", "value": 42}
```

### Dispatch writer

`OrcWriterDispatch` allow to write data encoded in folder or in file.

### Basic Type

The following code will encode data like `folder1=1/test-1-0.orc`
```cpp
OrcWriterMulti<FolderEncode<Int>, Int> writer({"folder1", "value"}, "data", "test", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(17875 + i/2., 42 + i);
}
```

Resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents) (`2018/12/10/date-2018-12-10-0.orc`):
```python
{"value": 42}
{"value": 43}
```

### Date Type

Date type have special behaviour encoded in folder. The following code create a file by day in a tree with the pattern `year={YYYY}/month={MM}/day={DD}/{prefix}-{YYYY}-{MM}-{DD}-{suffix}.orc`. (*Note*: in this case the column name for the date is not used)
```cpp
OrcWriterMulti<FolderEncode<DateNumber>, Int> writer({"date", "value"}, "data", "test", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(17875 + i/2., 42 + i);
}
```

Resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents) (`2018/12/10/date-2018-12-10-0.orc`):
```python
{"value": 42}
{"value": 43}
```

Resulting tree created:
```bash
data
├── year=2018
│   └── month=12
│       ├── day=10
│       │   └── test-2018-12-10-0.orc
│       ├── day=11
│       │   └── test-2018-12-11-0.orc
...
│       └── day=31
│           └── test-2018-12-31-0.orc
└── year=2019
    └── month=1
        ├── day=1
        │   └── test-2019-1-1-0.orc
        ├── day=2
        │   └── test-2019-1-2-0.orc
...
        ├── day=26
        │   └── test-2019-1-26-0.orc
        ├── day=27
        │   └── test-2019-1-27-0.orc
        └── day=28
            └── test-2019-1-28-0.orc
```

More example for dispatch writer can be found [here](example/cpp) or in [test](tests/cpp/unit-dispatch-folder-encoding.cpp)

### Sequential writer

`OrcWriterSequential` have globally the same behaviour the dispatch writer. But where `OrcDisptachWriter` keep all file open, `OrcWriterSequential` close file when its main key change. In the following example, at each new day all file are closed and finalized.

```cpp
OrcWriterMulti<DateNumber, FolderEncode<Int>, Int> writer({"date", folder2, "value"}, "data", "test", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(17875 + i/2., i%2, 42 + i);
}
```

More example for sequential writer can be found [here](example/cpp) or in [test](tests/cpp/unit-sequential-closing.cpp)

### Dispatch Duplicate writer

`OrcWriterDispatchDuplicate` we have to define a main key that appear in folder and in file.
The following code will encode data like `mainkey=1/folder1=17875/test-1-17875-0.orc`
```cpp
OrcWriterMulti<Int, FolderEncode<Int>, Int> writer({"mainkey", "folder1", "value"}, "data", "test", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(i%2, 17875 + i/2., 42 + i);
}
```

Resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents) :
```python
{"mainkey": 0, "value": 42}
{"mainkey": 0, "value": 44}
```
More example for sequential writer can be found [here](example/cpp) or in [test](tests/cpp/unit-dispatch-duplicate-timestampnumber.cpp)

### Sequential Duplicate writer

`OrcWriterSequentialDuplicate` the main key that appear in folder and in file.
The following code will encode data like `mainkey=1/folder1=17875/test-1-17875-0.orc`
```cpp
OrcWriterMulti<Int, FolderEncode<Int>, Int> writer({"mainkey", "folder1", "value"}, "data", "test", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(i%2, 17875 + i/2., 42 + i);
}
```

Resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents) :
```python
{"mainkey": 0, "value": 42}
{"mainkey": 0, "value": 44}
```

More example for sequential writer can be found [here](example/cpp) or in [test](tests/cpp/unit-sequential-duplicate-timestampnumber.cpp)


### Threaded file writer


Orc write in file by Batch. To avoid the writing time during a batch to periodicaly increase `write` method, I move the write process in a separate thread for `OrcWriterThread`.
This writer can use either `OrcWriterDispatchDuplicate` or `OrcWriterSequentialDuplicate`.
```cpp
    OrcWriterThread<OrcWriterDispatchDuplicate, FolderEncode<DateNumber>, Int> writer_multi({"A2", "B2"}, "data", "test", WriterOptions());
    for (int i = 0; i < 100; ++i) {
        writer.write(17875 + i/2., 42 + i);
    }
```

**Note**: The multi thread functionnality is performed by a simple FIFO with a provider and a consumer. I the provider is too fast the queue can be full and generate a core dump.


## Compilation, Tests and Documentations

We use [vcpkg](https://github.com/Microsoft/vcpkg) to manage dependencies

Stryke depend on:
* [Catch2](https://github.com/catchorg/Catch2)
* [Apache-Orc](https://orc.apache.org/)
* [Pybind11](https://github.com/pybind/pybind11) (for python binding)

```
./vcpkg install catch2 orc pybind11 zstd
```

### Compile Example, Test and/or Python binding

There is 4 options :
* BUILD_EXAMPLES (default: OFF)
* BUILD_UNIT_TESTS (default: OFF)
* BUILD_PYTHON_BINDING (default: OFF)
* CUSTOM_BINDING (default: `python/custom_binding_example.cpp`). More explaination on python binding [here](python).

```bash
mkdir build
cd build
# configure make with vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake -DBUILD_PYTHON_BINDING=ON -DBUILD_EXAMPLES=ON -DBUILD_UNIT_TESTS=ON
make
```

### Run Tests

```bash
make test
```

### Generate Doxygen documentation

You need to have doxygen and graphviz installed on your computer.

```bash
make docs
```


TODO:   - Mettre à jour le readme

TODO: - Fixer le writer csv et csv multifile

TODO: - Mettre à jour le readme général
TODO: - Ajouter plus de tests pour sequential
TODO: - Ajouter test pour le nom des colonnes
TODO: - Add comments in code
