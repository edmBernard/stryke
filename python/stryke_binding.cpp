
#include "stryke_multifile.hpp"
#include "stryke_template.hpp"
#include "stryke_thread.hpp"
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <string>
#include <iostream>
#include <array>


namespace py = pybind11;

// ==============================================================
// Type Convertion
// ==============================================================
namespace pybind11 { namespace detail {

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
//     return !(value.data == -1 && !PyErr_Occurred());
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
//     return !(value.data == -1 && !PyErr_Occurred());
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
    return !(value.data == -1 && !PyErr_Occurred());
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
//     return !(value.data == -1 && !PyErr_Occurred());
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
    return !(value.data == -1 && !PyErr_Occurred());
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
    return !(value.data == false && !PyErr_Occurred());
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
    PyObject* pyStr = PyUnicode_AsEncodedString(tmp, "utf-8", "Error ~");
    value.data = PyBytes_AsString(pyStr);
    Py_DECREF(tmp);
    return !PyErr_Occurred();
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
    return !(value.data == -1 && !PyErr_Occurred());
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
    PyObject* pyStr = PyUnicode_AsEncodedString(tmp, "utf-8", "Error ~");
    value.data = PyBytes_AsString(pyStr);
    Py_DECREF(tmp);
    return !PyErr_Occurred();
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
    return !(value.data == -1 && !PyErr_Occurred());
  }
};

}} // namespace pybind11::detail

// ==============================================================
// Template to automate template binding
// ==============================================================

template<typename... T>
void declare_writer_impl(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterImpl<T...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
    .def(py::init<std::array<std::string, sizeof...(T)>, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("filename"), py::arg("writer_options"))
    .def("write", (void (Class::*)(T...)) &Class::write);
}

template<typename... T>
void declare_writer_multifile(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterMulti<T...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
    .def(py::init<std::array<std::string, sizeof...(T)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
    .def("write", (void (Class::*)(T...)) &Class::write);
}

template<typename... T>
void declare_writer_thread(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterThread<stryke::OrcWriterMulti, T...>;
  std::string pyclass_name = std::string("Writer") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
    .def(py::init<std::array<std::string, sizeof...(T)>, std::string, std::string, const stryke::WriterOptions &>(), py::arg("column_names"), py::arg("root_folder"), py::arg("prefix"), py::arg("writer_options"))
    .def("write", (void (Class::*)(T...)) &Class::write);
}


PYBIND11_MODULE(stryke, m) {

  py::class_<stryke::WriterOptions>(m, "WriterOptions")
    .def(py::init<>())
    .def("disable_lock_file", &stryke::WriterOptions::disable_lock_file)
    .def("set_batch_size", &stryke::WriterOptions::set_batch_size)
    .def("set_batch_max", &stryke::WriterOptions::set_batch_max)
    .def("set_stripe_size", &stryke::WriterOptions::set_stripe_size);

// ==============================================================
// Binding for WriterTemplate
// ==============================================================

  auto m_template = m.def_submodule("template");

  // 1D Point
  declare_writer_impl<stryke::TimestampNumber, stryke::Long>(m_template, "TimestampPoint1l");
  declare_writer_impl<stryke::DateNumber, stryke::Long>(m_template, "DatePoint1l");
  declare_writer_impl<stryke::TimestampNumber, stryke::Double>(m_template, "TimestampPoint1d");
  declare_writer_impl<stryke::DateNumber, stryke::Double>(m_template, "DatePoint1d");

  // 2D Point
  declare_writer_impl<stryke::TimestampNumber, stryke::Long, stryke::Long>(m_template, "TimestampPoint2l");
  declare_writer_impl<stryke::DateNumber, stryke::Long, stryke::Long>(m_template, "DatePoint2l");
  declare_writer_impl<stryke::TimestampNumber, stryke::Double, stryke::Double>(m_template, "TimestampPoint2d");
  declare_writer_impl<stryke::DateNumber, stryke::Double, stryke::Double>(m_template, "DatePoint2d");

  // 3D Point
  declare_writer_impl<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long>(m_template, "TimestampPoint3l");
  declare_writer_impl<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long>(m_template, "DatePoint3l");
  declare_writer_impl<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double>(m_template, "TimestampPoint3d");
  declare_writer_impl<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double>(m_template, "DatePoint3d");

  // Pair of coordinate 2D Point
  declare_writer_impl<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_template, "TimestampVec2l");
  declare_writer_impl<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_template, "DateVec2l");
  declare_writer_impl<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_template, "TimestampVec2d");
  declare_writer_impl<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_template, "DateVec2d");

  // Pair of coordinate 3D Point
  declare_writer_impl<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_template, "TimestampVec3l");
  declare_writer_impl<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_template, "DateVec3l");
  declare_writer_impl<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_template, "TimestampVec3d");
  declare_writer_impl<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_template, "DateVec3d");

// ==============================================================
// Binding for WriterMultifile
// ==============================================================

  auto m_multifile = m.def_submodule("multifile");

  // 1D Point
  declare_writer_multifile<stryke::TimestampNumber, stryke::Long>(m_multifile, "TimestampPoint1l");
  declare_writer_multifile<stryke::DateNumber, stryke::Long>(m_multifile, "DatePoint1l");
  declare_writer_multifile<stryke::TimestampNumber, stryke::Double>(m_multifile, "TimestampPoint1d");
  declare_writer_multifile<stryke::DateNumber, stryke::Double>(m_multifile, "DatePoint1d");

  // 2D Point
  declare_writer_multifile<stryke::TimestampNumber, stryke::Long, stryke::Long>(m_multifile, "TimestampPoint2l");
  declare_writer_multifile<stryke::DateNumber, stryke::Long, stryke::Long>(m_multifile, "DatePoint2l");
  declare_writer_multifile<stryke::TimestampNumber, stryke::Double, stryke::Double>(m_multifile, "TimestampPoint2d");
  declare_writer_multifile<stryke::DateNumber, stryke::Double, stryke::Double>(m_multifile, "DatePoint2d");

  // 3D Point
  declare_writer_multifile<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "TimestampPoint3l");
  declare_writer_multifile<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "DatePoint3l");
  declare_writer_multifile<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "TimestampPoint3d");
  declare_writer_multifile<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "DatePoint3d");

  // Pair of coordinate 2D Point
  declare_writer_multifile<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "TimestampVec2l");
  declare_writer_multifile<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "DateVec2l");
  declare_writer_multifile<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "TimestampVec2d");
  declare_writer_multifile<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "DateVec2d");

  // Pair of coordinate 3D Point
  declare_writer_multifile<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "TimestampVec3l");
  declare_writer_multifile<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_multifile, "DateVec3l");
  declare_writer_multifile<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "TimestampVec3d");
  declare_writer_multifile<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_multifile, "DateVec3d");

// ==============================================================
// Binding for WriterThread
// ==============================================================

  auto m_thread = m.def_submodule("thread");

  // 1D Point
  declare_writer_thread<stryke::TimestampNumber, stryke::Long>(m_thread, "TimestampPoint1l");
  declare_writer_thread<stryke::DateNumber, stryke::Long>(m_thread, "DatePoint1l");
  declare_writer_thread<stryke::TimestampNumber, stryke::Double>(m_thread, "TimestampPoint1d");
  declare_writer_thread<stryke::DateNumber, stryke::Double>(m_thread, "DatePoint1d");

  // 2D Point
  declare_writer_thread<stryke::TimestampNumber, stryke::Long, stryke::Long>(m_thread, "TimestampPoint2l");
  declare_writer_thread<stryke::DateNumber, stryke::Long, stryke::Long>(m_thread, "DatePoint2l");
  declare_writer_thread<stryke::TimestampNumber, stryke::Double, stryke::Double>(m_thread, "TimestampPoint2d");
  declare_writer_thread<stryke::DateNumber, stryke::Double, stryke::Double>(m_thread, "DatePoint2d");

  // 3D Point
  declare_writer_thread<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long>(m_thread, "TimestampPoint3l");
  declare_writer_thread<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long>(m_thread, "DatePoint3l");
  declare_writer_thread<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double>(m_thread, "TimestampPoint3d");
  declare_writer_thread<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double>(m_thread, "DatePoint3d");

  // Pair of coordinate 2D Point
  declare_writer_thread<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_thread, "TimestampVec2l");
  declare_writer_thread<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_thread, "DateVec2l");
  declare_writer_thread<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_thread, "TimestampVec2d");
  declare_writer_thread<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_thread, "DateVec2d");

  // Pair of coordinate 3D Point
  declare_writer_thread<stryke::TimestampNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_thread, "TimestampVec3l");
  declare_writer_thread<stryke::DateNumber, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long, stryke::Long>(m_thread, "DateVec3l");
  declare_writer_thread<stryke::TimestampNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_thread, "TimestampVec3d");
  declare_writer_thread<stryke::DateNumber, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double, stryke::Double>(m_thread, "DateVec3d");

// ==============================================================
// Custom Binding
// ==============================================================

  auto m_custom = m.def_submodule("custom");

  declare_writer_thread<stryke::TimestampNumber, stryke::Long>(m_custom, "TimestampIntDouble");

}