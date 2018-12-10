# Stryke
A C++ template library build on top of Orc C++ library.

> The C++ Orc lib is highly dynamic. Data Schema is defined at runtime. As Orc have to be bind by dynamic language like python it's perfect. The goal of this lib is to make a more static/templated C++ lib. this code will try to make an easy to use writer and try to fix most of the structure at compilation time.

## Examples

### Simple writer

```cpp
    OrcWriterImpl<Date, Int> writer({"date", "value"}, "example.orc", 100000);
    writer.write(std::string("2018-12-10"), 42);
```

resulting file read by `orc-content`:
```python
{"date": "2018-12-09", "value": 42}
```

### Multi file writer

`OrcWriterMulti` create a file by day in a tree with the pattern `{YYYY}/{MM}/{DD}/{prefix}{YYYY}-{MM}-{DD}-{suffix}.orc`
```cpp
OrcWriterMulti<DateNumber, Int> writer({"date", "value"}, "data", "date_", 100000, 10);
for (int i = 0; i < 100; ++i) {
    writer.write(17875 + i/2., 42 + i);
}
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

```cpp
    OrcWriterThread<OrcWriterMulti, DateNumber, Int> writer_multi({"A2", "B2"}, "data", "date_", 1000000, 10);
    for (int i = 0; i < 100; ++i) {
        writer.write(17875 + i/2., 42 + i);
    }
```

**Note**: The multi thread functionnality is performed by a simple FIFO with a provider and a consumer. I the provider is too fast the queue can be full and generate a core dump.

## Compilation

We use [vcpkg](https://github.com/Microsoft/vcpkg) to manage dependencies

Stryke depend on:
* [Catch2](https://github.com/catchorg/Catch2)
* [Apache-Orc](https://orc.apache.org/)

```
./vcpkg install catch2 orc
```

### Compile

```bash
mkdir build
cd build
# configure make with vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake
make
```
