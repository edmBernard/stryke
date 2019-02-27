# Stryke

This is the python binding for the Stryke build on top of Apache-Orc C++ library. All code is available on [github](https://github.com/edmBernard/Stryke). It's a template library but depend on [Apache-Orc](https://orc.apache.org/).

Apache-Orc is defined as :
> the smallest, fastest columnar storage for Hadoop workloads.

> The straight forward way to use Orc file in Python is through PySpark.

## Defined Writer

As Stryke is a template Library we have to defined Writer template specialization at compilation.
For each C++ Writer, we have define some binding mainly for test purpose.
* `OrcWriterImpl` writers are in `simple` namespace
* `OrcWriterDispatch` writers are in `dispatch` namespace
* `OrcWriterSequential` writers are in `sequential` namespace
* `OrcWriterThread` writers are in `thread` namespace
* Your writers will be definer in `custom` namespace

## How to define new Custom Writer

This [file](custom_binding_example.cpp) show some example to define new writer.

### Option 1:

You can edit this [file](custom_binding_example.cpp) in place and recompile python binding. Your new binding will be available in the `stryke.custom` namespace (with a `Writer` prefix)

### Option 2:

You can copy this [file](custom_binding_example.cpp) elsewhere. Then to tell compiler where your binding is located, you have to define the `CUSTOM_BINDING=<path_to_your_file>` in the cmake command. After compilation your new binding will be available in the `stryke.custom` namespace (with a `Writer` prefix)

```
cmake .. -DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake -DBUILD_PYTHON_BINDING=ON -DCUSTOM_BINDING=<path_to_your_file>
```


## Compilation, Installation and Tests

We use [vcpkg](https://github.com/Microsoft/vcpkg) to manage dependencies

Stryke depend on:
* [Catch2](https://github.com/catchorg/Catch2)
* [Apache-Orc](https://orc.apache.org/)
* [Pybind11](https://github.com/pybind/pybind11)

```
./vcpkg install catch2 orc pybind11 zstd
```

### Compile C++ code and Python binding

```bash
#from root folder
mkdir build
cd build
# configure make with vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake -DBUILD_PYTHON_BINDING=ON
make install
```

The shared library (`stryke.so`) with the binding will be store in `python/stryke` folder.

### Install Binding

```bash
# from python folder
python3 setup.py install
```

### Run Python Tests

```bash
python3 -m pytest tests/python
```


## Examples

### Simple writer

The following python writer defined by :
```cpp
declare_writer_impl<TimestampNumber, Long, Long, Long>(m_simple, "ForTest");
```

```python
import stryke as sy
writer = sy.simple.WriterForTest(["date", "univers", "value1", "value2"], "example.orc", sy.WriterOptions())
writer.write(1544400000, 42, 10, 1)     # Timestamp Type is the number of second since 1970.
writer.write(1544400000.123456789, 42, 20, 2)  # Timestamp Type is the number of second since 1970.
# the file is close at writer destruction
```

resulting file read by `orc-content`:
```python
{"date": "2018-12-10 00:00:00.0", "value": 42}
{"date": "2018-12-10 00:00:00.123456716", "value": 42}
```


### Dispatch writer

The following python writer defined by :
```cpp
declare_writer_dispatch<FolderEncode<TimestampNumber, Long>, Long, Long>(m_dispatch, "ForTestWithFolder");
```

```python
import stryke as sy
writer = sy.dispatch.WriterForTestWithFolder(["date", "univers", "value1", "value2"], "data", "example", sy.WriterOptions())
writer.write(1544400000, 42, 10, 1)     # Timestamp Type is the number of second since 1970.
writer.write(1544400000.123456789, 42, 20, 2)  # Timestamp Type is the number of second since 1970.
# the file is close at writer destruction
```

resulting files read by `orc-content`:
```python
{"value1": 10, "value2": 1}
{"value1": 20, "value2": 2}
```

