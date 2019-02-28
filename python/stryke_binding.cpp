
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

void init_custom(py::module &);

PYBIND11_MODULE(stryke, m) {

  using namespace stryke; // for convenience

  py::class_<WriterOptions>(m, "WriterOptions")
    .def(py::init<>())
    .def("disable_lock_file", &WriterOptions::disable_lock_file, py::arg("disable_lock_file") = true)
    .def("enable_suffix_timestamp", &WriterOptions::enable_suffix_timestamp, py::arg("enable_suffix_timestamp") = true)
    .def("set_batch_size", &WriterOptions::set_batch_size)
    .def("set_batch_max", &WriterOptions::set_batch_max)
    .def("set_stripe_size", &WriterOptions::set_stripe_size)
    .def("set_cron", &WriterOptions::set_cron);

  // ==============================================================
  // Binding for WriterTemplate
  // ==============================================================

  auto m_simple = m.def_submodule("simple");

  // Example
  declare_writer_impl<TimestampNumber, Long, Long, Long>(m_simple, "ForTest");

  // Basic type
  declare_writer_impl<Long>(m_simple, "Long1");
  declare_writer_impl<Double>(m_simple, "Double1");
  declare_writer_impl<Boolean>(m_simple, "Boolean1");
  declare_writer_impl<Date>(m_simple, "Date1");
  declare_writer_impl<DateNumber>(m_simple, "DateN1");
  declare_writer_impl<Timestamp>(m_simple, "Timestamp1");
  declare_writer_impl<TimestampNumber>(m_simple, "TimestampN1");

  // Reader
  declare_reader<Long>(m_simple, "Long1");
  declare_reader<Double>(m_simple, "Double1");
  declare_reader<Boolean>(m_simple, "Boolean1");
  declare_reader<Date>(m_simple, "Date1");
  declare_reader<DateNumber>(m_simple, "DateN1");
  declare_reader<Timestamp>(m_simple, "Timestamp1");
  declare_reader<TimestampNumber>(m_simple, "TimestampN1");

  // ==============================================================
  // Binding for WriterDispatch
  // ==============================================================

  auto m_dispatch = m.def_submodule("dispatch");

  // Example
  declare_writer_dispatch<FolderEncode<TimestampNumber, Long>, Long, Long>(m_dispatch, "ForTestWithFolder");
  declare_writer_dispatch<FolderEncode<>, TimestampNumber, Long, Long, Long>(m_dispatch, "ForTestWithoutFolder");

  // ==============================================================
  // Binding for WriterSequential
  // ==============================================================

  auto m_sequential = m.def_submodule("sequential");

  // Example
  declare_writer_sequential<TimestampNumber, FolderEncode<Long>, Long, Long>(m_sequential, "ForTestWithFolder");
  declare_writer_sequential<TimestampNumber, FolderEncode<>, Long, Long>(m_sequential, "ForTestWithoutFolder");

  // ==============================================================
  // Binding for WriterThread
  // ==============================================================

  auto m_thread = m.def_submodule("thread");

  // Example
  declare_writer_thread<OrcWriterSequentialDuplicate, TimestampNumber, FolderEncode<Long>, Long, Long>(m_thread, "ForTestWithFolder");
  declare_writer_thread<OrcWriterSequentialDuplicate, TimestampNumber, FolderEncode<>, Long, Long, Long>(m_thread, "ForTestWithoutFolder");


  // ==============================================================
  // Binding for Miscellaneous
  // ==============================================================

    // ==============================================================
    // Binding for Miscellaneous WriterThreadDispatchDuplicate
    // ==============================================================
    auto m_dispatch_miscellaneous = m_dispatch.def_submodule("miscellaneous");

    // Example
    declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<Long>, Long, Long>(m_dispatch_miscellaneous, "ForTestWithFolder");
    declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<>, Long, Long>(m_dispatch_miscellaneous, "ForTestWithoutFolder");

    // ==============================================================
    // Binding for Miscellaneous WriterThreadSequentialDuplicate
    // ==============================================================
    auto m_sequential_miscellaneous = m_sequential.def_submodule("miscellaneous");

    // Example
    declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<Long>, Long, Long>(m_sequential_miscellaneous, "ForTestWithFolder");
    declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<>, Long, Long>(m_sequential_miscellaneous, "ForTestWithoutFolder");

  auto m_custom = m.def_submodule("custom");

  init_custom(m_custom);

}
