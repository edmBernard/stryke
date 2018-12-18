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
| `<Date, Long>` | `WriterDatePoint1l` |
| `<Timestamp, Long>` | `WriterTimestampPoint1l` |
| `<Date, Double>` | `WriterDatePoint1d` |
| `<Timestamp, Double>` | `WriterTimestampPoint1d` |
| `<Date, Long, Long>` | `WriterDatePoint2l` |
| `<Timestamp, Long, Long>` | `WriterTimestampPoint2l` |
| `<Date, Double, Double>` | `WriterDatePoint2d` |
| `<Timestamp, Double, Double>` | `WriterTimestampPoint2d` |
| `<Date, Long, Long, Long>` | `WriterDatePoint3l` |
| `<Timestamp, Long, Long, Long>` | `WriterTimestampPoint3l` |
| `<Date, Double, Double, Double>` | `WriterDatePoint3d` |
| `<Timestamp, Double, Double, Double>` | `WriterTimestampPoint3d` |
| `<Date, Long, Long, Long, Long>` | `WriterDateVec2l` |
| `<Timestamp, Long, Long, Long, Long>` | `WriterTimestampVec2l` |
| `<Date, Double, Double, Double, Double>` | `WriterDateVec2d` |
| `<Timestamp, Double, Double, Double, Double>` | `WriterTimestampVec2d` |
| `<Date, Long, Long, Long, Long, Long, Long>` | `WriterDateVec3l` |
| `<Timestamp, Long, Long, Long, Long, Long, Long>` | `WriterTimestampVec3l` |
| `<Date, Double, Double, Double, Double, Double, Double>` | `WriterDateVec3d` |
| `<Timestamp, Double, Double, Double, Double, Double, Double>` | `WriterTimestampVec3d` |
| `<DateNumber, Long>` | `WriterDateNPoint1l` |
| `<TimestampNumber, Long>` | `WriterTimestampNPoint1l` |
| `<DateNumber, Double>` | `WriterDateNPoint1d` |
| `<TimestampNumber, Double>` | `WriterTimestampNPoint1d` |
| `<DateNumber, Long, Long>` | `WriterDateNPoint2l` |
| `<TimestampNumber, Long, Long>` | `WriterTimestampNPoint2l` |
| `<DateNumber, Double, Double>` | `WriterDateNPoint2d` |
| `<TimestampNumber, Double, Double>` | `WriterTimestampNPoint2d` |
| `<DateNumber, Long, Long, Long>` | `WriterDateNPoint3l` |
| `<TimestampNumber, Long, Long, Long>` | `WriterTimestampNPoint3l` |
| `<DateNumber, Double, Double, Double>` | `WriterDateNPoint3d` |
| `<TimestampNumber, Double, Double, Double>` | `WriterTimestampNPoint3d` |
| `<DateNumber, Long, Long, Long, Long>` | `WriterDateNVec2l` |
| `<TimestampNumber, Long, Long, Long, Long>` | `WriterTimestampNVec2l` |
| `<DateNumber, Double, Double, Double, Double>` | `WriterDateNVec2d` |
| `<TimestampNumber, Double, Double, Double, Double>` | `WriterTimestampNVec2d` |
| `<DateNumber, Long, Long, Long, Long, Long, Long>` | `WriterDateNVec3l` |
| `<TimestampNumber, Long, Long, Long, Long, Long, Long>` | `WriterTimestampNVec3l` |
| `<DateNumber, Double, Double, Double, Double, Double, Double>` | `WriterDateNVec3d` |
| `<TimestampNumber, Double, Double, Double, Double, Double, Double>` | `WriterTimestampNVec3d` |

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

`multifile.Writer*` create a file by day in a tree with the pattern `year={YYYY}/month={MM}/day={DD}/{prefix}{YYYY}-{MM}-{DD}-{suffix}.orc`
```python
import stryke as sy
writer = sy.multifile.WriterDatePoint1l(["Date", "value"], "data", "date_", sy.WriterOptions())
for i in range(100):
    writer.write(17875 + i/2., 42 + i)
# the file is close at writer destruction
```

resulting file read by `orc-content` (`data/year=2018/month=12/day=10/date_2018-12-10-0.orc`):
```python
{"date": "2018-12-10", "value": 42}
{"date": "2018-12-10", "value": 43}
```

resulting tree created:
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
