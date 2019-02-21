
#include "stryke/core.hpp"
#include "stryke/dispatch.hpp"
#include "stryke/sequential.hpp"
#include "stryke/options.hpp"
#include "stryke/reader.hpp"
#include "stryke/thread.hpp"
#include "stryke/miscellaneous/dispatch_duplicate.hpp"
#include "stryke/miscellaneous/sequential_duplicate.hpp"
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <array>
#include <iostream>
#include <string>

namespace py = pybind11;

// ==============================================================
// Type Convertion
// ==============================================================
namespace pybind11 {
namespace detail {

// For safety reason as I'm not so sure of my binding,
// I will comment type with unsafe conversion like Short<->Long
// concern type are :
// Short
// Int
// Float

// For safety reason as I'm not so sure of my binding,
// I will comment type with unsafe conversion like Short<->Long
// template <>
// struct type_caster<stryke::Int> {
// public:
//   PYBIND11_TYPE_CASTER(stryke::Int, _("stryke.Int"));
//   bool load(handle src, bool) {
//     PyObject *source = src.ptr();
//     PyObject *tmp = PyNumber_Long(source);
//     if (!tmp)
//       return false;
//     value.data = PyLong_AsLong(tmp);  // it's maybe not safe to convert long to int
//     Py_DECREF(tmp);
//     return !(value.data == -1 && PyErr_Occurred());
//   }
// };

// For safety reason as I'm not so sure of my binding,
// I will comment type with unsafe conversion like Short<->Long
// template <>
// struct type_caster<stryke::Short> {
// public:
//   PYBIND11_TYPE_CASTER(stryke::Short, _("stryke.Short"));
//   bool load(handle src, bool) {
//     PyObject *source = src.ptr();
//     PyObject *tmp = PyNumber_Long(source);
//     if (!tmp)
//       return false;
//     value.data = PyLong_AsLong(tmp);  // it's maybe not safe to convert long to short
//     Py_DECREF(tmp);
//     return !(value.data == -1 && PyErr_Occurred());
//   }
// };

template <>
struct type_caster<stryke::Long> {
public:
  PYBIND11_TYPE_CASTER(stryke::Long, _("stryke.Long"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyNumber_Long(source);
    if (!tmp)
      return false;
    value.data = PyLong_AsLong(tmp);
    value.empty = false;
    Py_DECREF(tmp);
    return !((value.data == -1) && (PyErr_Occurred() != NULL));
  }

  static handle cast(stryke::Long src, return_value_policy, handle) {
    return PyLong_FromLong(src.data);
  }
};

// For safety reason as I'm not so sure of my binding,
// I will comment type with unsafe conversion like Short<->Long
// template <>
// struct type_caster<stryke::Float> {
// public:
//   PYBIND11_TYPE_CASTER(stryke::Float, _("stryke.Float"));
//   bool load(handle src, bool) {
//     PyObject *source = src.ptr();
//     PyObject *tmp = PyNumber_Float(source);
//     if (!tmp)
//       return false;
//     value.data = PyFloat_AsDouble(tmp);  // it's maybe not safe to convert double to float
//     Py_DECREF(tmp);
//     return !(value.data == -1 && PyErr_Occurred());
//   }
// };

template <>
struct type_caster<stryke::Double> {
public:
  PYBIND11_TYPE_CASTER(stryke::Double, _("stryke.Double"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyNumber_Float(source);
    if (!tmp)
      return false;
    value.data = PyFloat_AsDouble(tmp);
    value.empty = false;
    Py_DECREF(tmp);
    return !((value.data == -1.0) && (PyErr_Occurred() != NULL));
  }

  static handle cast(stryke::Double src, return_value_policy, handle) {
    return PyFloat_FromDouble(src.data);
  }
};

template <>
struct type_caster<stryke::Boolean> {
public:
  PYBIND11_TYPE_CASTER(stryke::Boolean, _("stryke.Boolean"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    if (PyObject_IsTrue(source) == 1) {
      value.data = true;
    } else {
      value.data = false;
    }
    value.empty = false;
    return !((value.data == false) && (PyErr_Occurred() != NULL));
  }

  static handle cast(stryke::Boolean src, return_value_policy, handle) {
    if (src.data == true) {
      return Py_True;
    } else {
      return Py_False;
    }
  }
};

template <>
struct type_caster<stryke::Date> {
public:
  PYBIND11_TYPE_CASTER(stryke::Date, _("stryke.Date"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyObject_Str(source);
    if (!tmp)
      return false;
    PyObject *pyStr = PyUnicode_AsEncodedString(tmp, "utf-8", "Error ~");
    value.data = PyBytes_AsString(pyStr);
    Py_DECREF(tmp);
    return !PyErr_Occurred();
  }

  static handle cast(stryke::Date src, return_value_policy, handle) {
    return PyUnicode_FromString(src.data.c_str());
  }
};

template <>
struct type_caster<stryke::DateNumber> {
public:
  PYBIND11_TYPE_CASTER(stryke::DateNumber, _("stryke.DateNumber"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyNumber_Long(source);
    if (!tmp)
      return false;
    value.data = PyLong_AsLong(tmp);
    value.empty = false;
    Py_DECREF(tmp);
    return !((value.data == -1) && (PyErr_Occurred() != NULL));
  }

  static handle cast(stryke::DateNumber src, return_value_policy, handle) {
    return PyLong_FromLong(src.data);
  }
};

template <>
struct type_caster<stryke::Timestamp> {
public:
  PYBIND11_TYPE_CASTER(stryke::Timestamp, _("stryke.Timestamp"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyObject_Str(source);
    if (!tmp)
      return false;
    PyObject *pyStr = PyUnicode_AsEncodedString(tmp, "utf-8", "Error ~");
    value.data = PyBytes_AsString(pyStr);
    Py_DECREF(tmp);
    return !PyErr_Occurred();
  }

  static handle cast(stryke::Timestamp src, return_value_policy, handle) {
    return PyUnicode_FromString(src.data.c_str());
  }
};

template <>
struct type_caster<stryke::TimestampNumber> {
public:
  PYBIND11_TYPE_CASTER(stryke::TimestampNumber, _("stryke.TimestampNumber"));
  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyNumber_Float(source);
    if (!tmp)
      return false;
    value.data = PyFloat_AsDouble(tmp);
    value.empty = false;
    Py_DECREF(tmp);
    return !((value.data == -1.0) && (PyErr_Occurred() != NULL));
  }

  static handle cast(stryke::TimestampNumber src, return_value_policy, handle) {
    return PyFloat_FromDouble(src.data);
  }
};

} // namespace detail
} // namespace pybind11

// ==============================================================
// Template to automate template binding
// ==============================================================

template <typename... T>
void declare_writer_impl(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterImpl<T...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, sizeof...(T)>, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("filename"), py::arg("writer_options"))
      .def("write", (void (Class::*)(T...)) & Class::write);
}

template <typename TypesFolder, typename... Types>
void declare_writer_dispatch(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterDispatch<TypesFolder, Types...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, TypesFolder::size + sizeof...(Types)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
      .def("write", (void (Class::*)(TypesFolder, Types...)) & Class::write)
      .def("close", (void (Class::*)(TypesFolder, Types...)) & Class::close);
}

template <typename S, typename TypesFolder, typename... Types>
void declare_writer_sequential(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterSequential<S, TypesFolder, Types...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, 1 + TypesFolder::size + sizeof...(Types)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
      .def("write", (void (Class::*)(S, TypesFolder, Types...)) & Class::write)
      .def("close", (void (Class::*)(S, TypesFolder, Types...)) & Class::close);
}

template <typename S, typename TypesFolder, typename... Types>
void declare_writer_dispatch_duplicate(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterDispatchDuplicate<S, TypesFolder, Types...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, 1 + TypesFolder::size + sizeof...(Types)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
      .def("write", (void (Class::*)(S, TypesFolder, Types...)) & Class::write)
      .def("close", (void (Class::*)(S, TypesFolder, Types...)) & Class::close);
}

template <typename S, typename TypesFolder, typename... Types>
void declare_writer_sequential_duplicate(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterSequentialDuplicate<S, TypesFolder, Types...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, 1 + TypesFolder::size + sizeof...(Types)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
      .def("write", (void (Class::*)(S, TypesFolder, Types...)) & Class::write)
      .def("close", (void (Class::*)(S, TypesFolder, Types...)) & Class::close);
}

template <template <typename TypesFolder, typename... Types> typename Writer, typename S, typename TypesFolder, typename... Types>
void declare_writer_thread(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterThread<Writer, S, TypesFolder, Types...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::array<std::string, 1 + TypesFolder::size + sizeof...(Types)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
      .def("write", (void (Class::*)(S, TypesFolder, Types...)) & Class::write)
      .def("close_async", (void (Class::*)(S, TypesFolder, Types...)) & Class::close_async)
      .def("close_sync", (void (Class::*)(S, TypesFolder, Types...)) & Class::close_sync);
}


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

  // Basic type
  declare_writer_impl<Long>(m_simple, "Long1");
  declare_writer_impl<Double>(m_simple, "Double1");
  declare_writer_impl<Boolean>(m_simple, "Boolean1");
  declare_writer_impl<Date>(m_simple, "Date1");
  declare_writer_impl<DateNumber>(m_simple, "DateN1");
  declare_writer_impl<Timestamp>(m_simple, "Timestamp1");
  declare_writer_impl<TimestampNumber>(m_simple, "TimestampN1");

  // Reader
  m_simple.def("readerLong1", &orcReader<Long>, "Reader for single column Long");
  m_simple.def("readerDouble1", &orcReader<Double>, "Reader for single column Double");
  m_simple.def("readerBoolean1", &orcReader<Boolean>, "Reader for single column Boolean");
  m_simple.def("readerDate1", &orcReader<Date>, "Reader for single column Date");
  m_simple.def("readerDateN1", &orcReader<DateNumber>, "Reader for single column DateN");
  m_simple.def("readerTimestamp1", &orcReader<Timestamp>, "Reader for single column Timestamp");
  m_simple.def("readerTimestampN1", &orcReader<TimestampNumber>, "Reader for single column TimestampN");

  // ==============================================================
  // Binding for WriterDispatch
  // ==============================================================

  auto m_dispatch = m.def_submodule("dispatch");

  // Example
  declare_writer_dispatch<FolderEncode<TimestampNumber, Int>, Long, Long>( m_dispatch, "ForTestWithFolder");
  declare_writer_dispatch<FolderEncode<>, TimestampNumber, Int, Long, Long>( m_dispatch, "ForTestWithoutFolder");

  // ==============================================================
  // Binding for WriterSequential
  // ==============================================================

  auto m_sequential = m.def_submodule("sequential");

  // Example
  declare_writer_sequential<TimestampNumber, FolderEncode<Int>, Long, Long>(m_sequential, "ForTestWithFolder");
  declare_writer_sequential<TimestampNumber, FolderEncode<>, Long, Long>(m_sequential, "ForTestWithoutFolder");

  // ==============================================================
  // Binding for WriterThread
  // ==============================================================

  auto m_thread = m.def_submodule("thread");

  // Example
  // declare_writer_thread<OrcWriterSequential, TimestampNumber, FolderEncode<Int>, Long, Long>(m_thread, "ForTestWithFolder");
  // declare_writer_thread<OrcWriterSequential, TimestampNumber, FolderEncode<>, Int, Long, Long>(m_thread, "ForTestWithoutFolder");

  // ==============================================================
  // Binding for Miscellaneous
  // ==============================================================

    // ==============================================================
    // Binding for Miscellaneous WriterThreadDispatchDuplicate
    // ==============================================================
    auto m_dispatch_miscellaneous = m_dispatch.def_submodule("miscellaneous");

    // Example
    declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<Int>, Long, Long>(m_dispatch_miscellaneous, "ForTestWithFolder");
    declare_writer_dispatch_duplicate<TimestampNumber, FolderEncode<>, Long, Long>(m_dispatch_miscellaneous, "ForTestWithoutFolder");

    // ==============================================================
    // Binding for Miscellaneous WriterThreadSequentialDuplicate
    // ==============================================================
    auto m_sequential_miscellaneous = m_sequential.def_submodule("miscellaneous");

    // Example
    declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<Int>, Long, Long>(m_sequential_miscellaneous, "ForTest");
    declare_writer_sequential_duplicate<TimestampNumber, FolderEncode<>, Long, Long>(m_sequential_miscellaneous, "ForTest");

  // ==============================================================
  // Custom Binding
  // ==============================================================

  auto m_custom = m.def_submodule("custom");

}