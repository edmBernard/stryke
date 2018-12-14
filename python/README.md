# Stryke

This is the python bing for a C++ template library build on top of Orc C++ library. Code is available on github [here](https://github.com/edmBernard/Stryke). It's a template library but depend on [Apache-Orc](https://orc.apache.org/). All headers are in `include` folder.

> The straight forward way to use Orc file in Python is through PySpark.

## Defined Writer

As Stryke is a template Library we have to defined Writer template specialization at compilation. We already define the following type :

| C++ Writer (in stryke namespace) | Corresponding Python Writer (in stryke namespace)|
|--|--|
| `OrcWriterImpl<DateNumber, Long>` | `template.WriterPoint1l` |
| `OrcWriterImpl<DateNumber, Double>` | `template.WriterPoint1d` |
| `OrcWriterImpl<DateNumber, Long, Long>` | `template.WriterPoint2l` |
| `OrcWriterImpl<DateNumber, Double, Double>` | `template.WriterPoint2d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long>` | `template.WriterPoint3l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double>` | `template.WriterPoint3d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long, Long>` | `template.WriterVec2l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double, Double>` | `template.WriterVec2d` |
| `OrcWriterImpl<DateNumber, Long, Long, Long, Long, Long, Long>` | `template.WriterVec3l` |
| `OrcWriterImpl<DateNumber, Double, Double, Double, Double, Double, Double>` | `template.WriterVec3d` |
| `OrcWriterMultifile<DateNumber, Long>` | `multifile.WriterPoint1l` |
| `OrcWriterMultifile<DateNumber, Double>` | `multifile.WriterPoint1d` |
| `OrcWriterMultifile<DateNumber, Long, Long>` | `multifile.WriterPoint2l` |
| `OrcWriterMultifile<DateNumber, Double, Double>` | `multifile.WriterPoint2d` |
| `OrcWriterMultifile<DateNumber, Long, Long, Long>` | `multifile.WriterPoint3l` |
| `OrcWriterMultifile<DateNumber, Double, Double, Double>` | `multifile.WriterPoint3d` |
| `OrcWriterMultifile<DateNumber, Long, Long, Long, Long>` | `multifile.WriterVec2l` |
| `OrcWriterMultifile<DateNumber, Double, Double, Double, Double>` | `multifile.WriterVec2d` |
| `OrcWriterMultifile<DateNumber, Long, Long, Long, Long, Long, Long>` | `multifile.WriterVec3l` |
| `OrcWriterMultifile<DateNumber, Double, Double, Double, Double, Double, Double>` | `multifile.WriterVec3d` |
| `OrcWriterThread<DateNumber, Long>` | `thread.WriterPoint1l` |
| `OrcWriterThread<DateNumber, Double>` | `thread.WriterPoint1d` |
| `OrcWriterThread<DateNumber, Long, Long>` | `thread.WriterPoint2l` |
| `OrcWriterThread<DateNumber, Double, Double>` | `thread.WriterPoint2d` |
| `OrcWriterThread<DateNumber, Long, Long, Long>` | `thread.WriterPoint3l` |
| `OrcWriterThread<DateNumber, Double, Double, Double>` | `thread.WriterPoint3d` |
| `OrcWriterThread<DateNumber, Long, Long, Long, Long>` | `thread.WriterVec2l` |
| `OrcWriterThread<DateNumber, Double, Double, Double, Double>` | `thread.WriterVec2d` |
| `OrcWriterThread<DateNumber, Long, Long, Long, Long, Long, Long>` | `thread.WriterVec3l` |
| `OrcWriterThread<DateNumber, Double, Double, Double, Double, Double, Double>` | `thread.WriterVec3d` |

## Examples

### Simple writer

```python
import stryke as sy
writer = sy.template.WriterPoint1l(["Date", "value"], "example.orc", sy.WriterOptions())
writer.write(17874, 42)  # at this time Date is the number of date from 1970.
# the file is close at writer destruction
```

resulting file read by `orc-content`:
```python
{"date": "2018-12-09", "value": 42}
```

### Multi file writer

`multifile.Writer*` create a file by day in a tree with the pattern `{YYYY}/{MM}/{DD}/{prefix}{YYYY}-{MM}-{DD}-{suffix}.orc`
```python
import stryke as sy
writer = sy.multifile.WriterPoint1l(["Date", "value"], "data", "date_", sy.WriterOptions())
for i in range(100):
    writer.write(17875 + i/2., 42 + i)
# the file is close at writer destruction
```

resulting file read by `orc-content` (`2018/12/10/date_2018-12-10-0.orc`):
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
writer = sy.thread.WriterPoint1l(["Date", "value"], "data", "date_", sy.WriterOptions())
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
