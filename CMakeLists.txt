project(Stryke)

#**************************************************************************************************
# General cMake settings
#**************************************************************************************************
cmake_minimum_required(VERSION 3.5)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
  cmake_policy(SET CMP0007 NEW)
endif(COMMAND cmake_policy)

if(NOT DEFINED CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
  set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE INTERNAL "No dev warnings")
endif()

#**************************************************************************************************
# Find Package ************************************************************************************
find_package(protobuf CONFIG REQUIRED)
find_library(LZ4_LIBRARY NAMES lz4 liblz4)
find_library(ZSTD_LIBRARY NAMES zstd)
find_package (ZLIB REQUIRED)
find_package(Snappy CONFIG REQUIRED)

find_library(ORC_LIBRARY NAMES orc liborc)

find_package(Catch2 CONFIG REQUIRED)

# get_cmake_property(_variableNames VARIABLES)
# list (SORT _variableNames)
# foreach (_variableName ${_variableNames})
#     message(STATUS "${_variableName}=${${_variableName}}")
# endforeach()

option(BUILD_PYTHON_BINDING "Build the unit tests" OFF)
set(CUSTOM_BINDING ${CMAKE_CURRENT_SOURCE_DIR}/python/custom_binding_example.cpp CACHE STRING "Build binding in the specified cpp")

if(BUILD_PYTHON_BINDING)

  # Pybind11
  # Pybind11 also set PYTHON_INCLUDE_DIRS and PYTHON_LIBRARIES
  find_package(pybind11 CONFIG REQUIRED)
  include_directories(${pybind11_INCLUDE_DIR})
  MESSAGE( " *** PYTHON_INCLUDE_DIRS : " ${PYTHON_INCLUDE_DIRS} )
  MESSAGE( " *** PYTHON_LIBRARIES : " ${PYTHON_LIBRARIES} )
  include_directories(${PYTHON_INCLUDE_DIRS})
endif()

#**************************************************************************************************
# Include *****************************************************************************************
include_directories(${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/python)


#**************************************************************************************************
# Set variable ************************************************************************************
SET(SOURCES

)

set(examples
  core_writer
  csv_writer
  csv_multifile
  dispatch
  dispatch_duplicate
  dispatch_folder
  merge_file
  performance
  reader_basic_type
  reader_rainbow
  sequential
  sequential_duplicate
  thread
  writer_rainbow
)

#**************************************************************************************************
# Set compiler ************************************************************************************
add_compile_options(-std=c++17 -Wall -fPIC) # for all targets in current directory

#**************************************************************************************************
# Linker ******************************************************************************************


#**************************************************************************************************
# Build Example
#**************************************************************************************************
option(BUILD_EXAMPLES "Build stryke example" OFF)
if(BUILD_EXAMPLES)
  foreach (example ${examples})
    add_executable("stryke_${example}" ${CMAKE_CURRENT_SOURCE_DIR}/example/cpp/${example}.cpp)
    target_compile_options("stryke_${example}" PRIVATE -O3)
    target_link_libraries("stryke_${example}" PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})
  endforeach(example)
endif()

#**************************************************************************************************
# Build Tests
#**************************************************************************************************
option(BUILD_UNIT_TESTS "Build the unit tests" OFF)
if(BUILD_UNIT_TESTS)
  enable_testing()

  SET(TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-template-type.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-template-batch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-template-lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-template-folder-creation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-template-col-name.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-dispatch-folder-encoding.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-dispatch-split.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-dispatch-col-name.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-sequential-closing.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-dispatch-duplicate-timestampnumber.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-sequential-duplicate-datenumber.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-sequential-duplicate-timestampnumber.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-thread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-thread-writer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cpp/unit-thread-folder-encoding.cpp
  )

  add_executable(unit-test ${TEST_SOURCES} ${SOURCES} ${TEST_HEADERS})
  target_link_libraries(unit-test PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

  add_test(NAME "unit_test_default"
    COMMAND unit-test
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

#**************************************************************************************************
# Build Python Binding
#**************************************************************************************************
if(BUILD_PYTHON_BINDING)
  add_library(stryke SHARED ${CMAKE_CURRENT_SOURCE_DIR}/python/stryke_binding.cpp ${CUSTOM_BINDING})
  target_link_libraries(stryke PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY} ${PYTHON_LIBRARIES})
  set_target_properties(stryke PROPERTIES PREFIX "")

  install(TARGETS stryke DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/python/stryke)
endif()

#**************************************************************************************************
# Documentation generation
#**************************************************************************************************

add_custom_target(
  docs
  COMMAND doxygen ${CMAKE_CURRENT_SOURCE_DIR}/docs/config_file
)
