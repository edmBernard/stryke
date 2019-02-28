
#include "stryke/type.hpp"
#include "stryke/core.hpp"
#include "stryke/dispatch.hpp"
#include "stryke/sequential.hpp"
#include "stryke/options.hpp"
#include "stryke/reader.hpp"
#include "stryke/thread.hpp"
#include "stryke/miscellaneous/dispatch_duplicate.hpp"
#include "stryke/miscellaneous/sequential_duplicate.hpp"
#include "stryke_binding.hpp"

#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <array>
#include <iostream>
#include <string>

namespace py = pybind11;

void init_custom(py::module &m) {

  using namespace stryke; // for convenience

  // We can't declare two time the same cpp type for example the following two line :
  //      declare_writer_impl<TimestampNumber, Long, Long>(m, "FirstDeclaration");
  //      declare_writer_impl<TimestampNumber, Long, Long>(m, "SecondDeclarationThatCrash");
  // will generate the following error at python import :
  //      ImportError: generic_type: type "WriterCustom1" is already registered!

  declare_writer_impl<TimestampNumber, Long, Long>(m, "Custom1");

  declare_writer_dispatch<FolderEncode<TimestampNumber, Long>, Long>(m, "Custom2");
  declare_writer_dispatch<FolderEncode<>, TimestampNumber, Long, Long>(m, "Custom3");

  declare_writer_sequential<TimestampNumber, FolderEncode<Long>, Long>(m, "Custom4");
  declare_writer_sequential<TimestampNumber, FolderEncode<>, Long>(m, "Custom5");

  declare_writer_thread<OrcWriterDispatchDuplicate, TimestampNumber, FolderEncode<Long>, Long>(m, "Custom6_1");
  declare_writer_thread<OrcWriterDispatchDuplicate, TimestampNumber, FolderEncode<>, Long, Long>(m, "Custom7_1");
  declare_writer_thread<OrcWriterSequentialDuplicate, TimestampNumber, FolderEncode<Long>, Long>(m, "Custom6_2");
  declare_writer_thread<OrcWriterSequentialDuplicate, TimestampNumber, FolderEncode<>, Long, Long>(m, "Custom7_2");

  declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<Long>, Long>(m, "Custom8");
  declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<>, Long>(m, "Custom9");

  declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<Long>, Long>(m, "Custom10");
  declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<>, Long>(m, "Custom11");

}
