

//
//  Stryke
//
//  Url
//
//  Created by Erwan BERNARD on 21/02/2019.
//
//  Copyright (c) 2019 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#pragma once
#ifndef STRYKE_BINDING_FOR_TEST_HPP_
#define STRYKE_BINDING_FOR_TEST_HPP_

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

template <typename... T>
void declare_reader(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcReader<T...>;
  std::string pyclass_name = std::string("Reader") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
      .def(py::init<std::string>(), py::arg("filename"))
      .def("get_data", &Class::get_data)
      .def("get_cols_name", &Class::get_cols_name);
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

#endif // !STRYKE_BINDING_FOR_TEST_HPP_
