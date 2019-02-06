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

### Simple writer

```cpp
OrcWriterImpl<Date, Int> writer({"date", "value"}, "example.orc", WriterOptions());
writer.write(std::string("2018-12-10"), 42);
```

resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents):
```python
{"date": "2018-12-09", "value": 42}
```

### Multi file writer

`OrcWriterMulti` create a file by day in a tree with the pattern `year={YYYY}/month={MM}/day={DD}/{prefix}{YYYY}-{MM}-{DD}-{suffix}.orc`

**Note**: The first Type must be a date. Currently, we only support `DateNumber` and `TimestampNumber`.

```cpp
OrcWriterMulti<DateNumber, Int> writer({"date", "value"}, "data", "date_", WriterOptions());
for (int i = 0; i < 100; ++i) {
    writer.write(17875 + i/2., 42 + i);
}
```

Resulting file read by [orc-content](https://orc.apache.org/docs/cpp-tools.html#orc-contents) (`2018/12/10/date_2018-12-10-0.orc`):
```python
{"date": "2018-12-10", "value": 42}
{"date": "2018-12-10", "value": 43}
```

Resulting tree created:
```bash
data
├── year=2018
│   └── month=12
│       ├── day=10
│       │   └── date_2018-12-10-0.orc
│       ├── day=11
│       │   └── date_2018-12-11-0.orc
...
│       └── day=31
│           └── date_2018-12-31-0.orc
└── year=2019
    └── month=1
        ├── day=1
        │   └── date_2019-1-1-0.orc
        ├── day=2
        │   └── date_2019-1-2-0.orc
...
        ├── day=26
        │   └── date_2019-1-26-0.orc
        ├── day=27
        │   └── date_2019-1-27-0.orc
        └── day=28
            └── date_2019-1-28-0.orc
```

### Threaded file writer

Orc write in file by Batch. To avoid the writing time during a batch to periodicaly increase `write` method, I move the write process in a separate for `OrcWriterThread`.
```cpp
    OrcWriterThread<OrcWriterMulti, DateNumber, Int> writer_multi({"A2", "B2"}, "data", "date_", WriterOptions());
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

There is 3 options :
* BUILD_PYTHON_BINDING (default: OFF)
* BUILD_EXAMPLES (default: OFF)
* BUILD_UNIT_TESTS (default: OFF)

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
