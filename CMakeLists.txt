project(Stryke)

#**************************************************************************************************
# General cMake settings
#**************************************************************************************************
cmake_minimum_required(VERSION 3.5)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

#**************************************************************************************************
# Find Package **************************************************************************************************
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

#**************************************************************************************************
# Include **************************************************************************************************
include_directories(${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)


#**************************************************************************************************
# Set variable **************************************************************************************************
SET(SOURCES

)


#**************************************************************************************************
# Set compiler **************************************************************************************************
add_compile_options(-std=c++17 -Wall -fPIC) # for all targets in current directory

#**************************************************************************************************
# Linker **************************************************************************************************


#**************************************************************************************************
# Make configuration
#**************************************************************************************************
add_executable(stryke_perf ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/main_performance.cpp)
target_compile_options(stryke_perf PRIVATE -O3)
target_link_libraries(stryke_perf PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

add_executable(stryke_thread ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/main_thread.cpp)
target_compile_options(stryke_thread PRIVATE -O3)
target_link_libraries(stryke_thread PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

add_executable(stryke_multifile ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/main_multifile.cpp)
target_compile_options(stryke_multifile PRIVATE -O3)
target_link_libraries(stryke_multifile PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

add_executable(stryke_dispatch ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/main_dispatch.cpp)
target_compile_options(stryke_dispatch PRIVATE -O3)
target_link_libraries(stryke_dispatch PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

add_executable(stryke_tpl ${SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/src/main_template.cpp)
target_compile_options(stryke_tpl PRIVATE -O3 -g)
target_link_libraries(stryke_tpl PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

#**************************************************************************************************
# Test configuration
#**************************************************************************************************
option(BUILD_UNIT_TESTS "Build the unit tests" ON)
if(BUILD_UNIT_TESTS)
  enable_testing()

  SET(TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit-main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit-template.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit-multifile.cpp
  )

  add_executable(unit-test ${TEST_SOURCES} ${SOURCES} ${TEST_HEADERS})
  target_link_libraries(unit-test PRIVATE -lstdc++fs ${ORC_LIBRARY} Snappy::snappy ZLIB::ZLIB protobuf::libprotoc protobuf::libprotobuf ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

  add_test(NAME "unit_test_default"
    COMMAND unit-test
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

