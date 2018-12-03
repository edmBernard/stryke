# Stryke
WIP: A C++ library build on top of Orc C++ library.

> The C++ Orc lib is highly dynamic. Data Schema is defined at runtime. As Orc have to be bind by dynmic language like python it's perfect. The goal of this lib is to make a more static/templated C++ lib. this code will try to make an easy to use writer and try to fix most of the structure at compilation time.

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
