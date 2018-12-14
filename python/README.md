# Stryke

This is the python binding for the Stryke build on top of Apache-Orc C++ library. All code is available on [github](https://github.com/edmBernard/Stryke). It's a template library but depend on [Apache-Orc](https://orc.apache.org/).

Apache-Orc is defined as :
> the smallest, fastest columnar storage for Hadoop workloads.

> The straight forward way to use Orc file in Python is through PySpark.

## Defined Writer

As Stryke is a template Library we have to defined Writer template specialization at compilation.
For each C++ Writer, we have define some useful datatype. Each C++ Writers are separate by namespace.
* `OrcWriterImpl` writers are in `template` namespace
* `OrcWriterMulitfile` writers are in `multifile` namespace
* `OrcWriterThread` writers are in `thread` namespace

We already define the following type for each Writer :

| C++ Types | Corresponding Python Writer |
|--|--|
| `OrcWriterImpl<DateNumber, Long>` | `WriterDatePoint1l` |
| `OrcWriterImpl<TimestampNumber, Long>` | `WriterTimestampPoint1l` |
| `OrcWriterImpl<DateNumber, Double>` | `WriterDatePoint1d` |
| `OrcWriterImpl<TimestampNumber, Double>` | `WriterTimestampPoint1d` |
| `OrcWriterImpl<DateNumber, Long, Long>` | `WriterDatePoint2l` |
| `OrcWriterImpl<TimestampNumber, Long, Long>` | `WriterTimestampPoint2l` |
| `OrcWriterImpl<DateNumber, Double, Double>` | `WriterDatePoint2d` |
| `OrcWriterImpl<TimestampNumber, Double, Double>` | `WriterTimestampPoint2d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long>` | `WriterDatePoint3l` |
| `OrcWriterImpl<TimestampNumber, Long, Long, Long>` | `WriterTimestampPoint3l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double>` | `WriterDatePoint3d` |
| `OrcWriterImpl<TimestampNumber, Double, Double, Double>` | `WriterTimestampPoint3d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long, Long>` | `WriterDateVec2l` |
| `OrcWriterImpl<TimestampNumber, Long, Long, Long, Long>` | `WriterTimestampVec2l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double, Double>` | `WriterDateVec2d` |
| `OrcWriterImpl<TimestampNumber, Double, Double, Double, Double>` | `WriterTimestampVec2d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long, Long, Long, Long>` | `WriterDateVec3l` |
| `OrcWriterImpl<TimestampNumber, Long, Long, Long, Long, Long, Long>` | `WriterTimestampVec3l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double, Double, Double, Double>` | `WriterDateVec3d` |
| `OrcWriterImpl<TimestampNumber, Double, Double, Double, Double, Double, Double>` | `WriterTimestampVec3d` |


## Examples

### Simple writer

#### Date

```python
import stryke as sy
writer = sy.template.WriterDatePoint1l(["Date", "value"], "example.orc", sy.WriterOptions())
writer.write(17874, 42)  # Date Type is the number of day since 1970.
# the file is close at writer destruction
```

resulting file read by `orc-content`:
```python
{"date": "2018-12-09", "value": 42}
```

#### Timestamp

```python
import stryke as sy
writer = sy.template.WriterTimestampPoint1l(["Date", "value"], "example.orc", sy.WriterOptions())
writer.write(1544400000, 42)  # Timestamp Type is the number of second since 1970.
writer.write(1544400000.123456789, 42)  # Timestamp Type is the number of second since 1970.
# the file is close at writer destruction
```

resulting file read by `orc-content`:
```python
{"Date": "2018-12-10 00:00:00.0", "value": 42}
{"Date": "2018-12-10 00:00:00.123456716", "value": 42}
```

### Multi file writer

`multifile.Writer*` create a file by day in a tree with the pattern `{YYYY}/{MM}/{DD}/{prefix}{YYYY}-{MM}-{DD}-{suffix}.orc`
```python
import stryke as sy
writer = sy.multifile.WriterDatePoint1l(["Date", "value"], "data", "date_", sy.WriterOptions())
for i in range(100):
    writer.write(17875 + i/2., 42 + i)
# the file is close at writer destruction
```

resulting file read by `orc-content` (`data/2018/12/10/date_2018-12-10-0.orc`):
```python
{"date": "2018-12-10", "value": 42}
{"date": "2018-12-10", "value": 43}
```

resulting tree created:
```bash
data
├── 2018
│   └── 12
│       ├── 10
│       │   └── date_2018-12-10-0.orc
│       ├── 11
│       │   └── date_2018-12-11-0.orc
...
│       └── 31
│           └── date_2018-12-31-0.orc
└── 2019
    └── 1
        ├── 1
        │   └── date_2019-1-1-0.orc
        ├── 2
        │   └── date_2019-1-2-0.orc
...
        ├── 26
        │   └── date_2019-1-26-0.orc
        ├── 27
        │   └── date_2019-1-27-0.orc
        └── 28
            └── date_2019-1-28-0.orc
```

### Threaded file writer

```python
import stryke as sy
writer = sy.thread.WriterDatePoint1l(["Date", "value"], "data", "date_", sy.WriterOptions())
for i in range(100):
    writer.write(17875 + i/2., 42 + i)
# the file is close at writer destruction
```

**Note**: The multi thread functionnality is performed by a simple FIFO with a provider and a consumer. I the provider is too fast the queue can be full and generate a core dump.

## Compilation, Installation and Tests

We use [vcpkg](https://github.com/Microsoft/vcpkg) to manage dependencies

Stryke depend on:
* [Catch2](https://github.com/catchorg/Catch2)
* [Apache-Orc](https://orc.apache.org/)

```
./vcpkg install catch2 orc
```

### Compile C++ code and Python binding

```bash
#from root folder
mkdir build
cd build
# configure make with vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake -DBUILD_PYTHON_BINDING
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
