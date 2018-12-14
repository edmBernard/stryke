
#include "stryke_template.hpp"
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

template <>
struct type_caster<stryke::Int> {
public:

  PYBIND11_TYPE_CASTER(stryke::Int, _("stryke.Int"));

  bool load(handle src, bool) {
    PyObject *source = src.ptr();
    PyObject *tmp = PyNumber_Long(source);
    if (!tmp)
      return false;
    value.data = PyLong_AsLong(tmp);
    Py_DECREF(tmp);
    return !(value.data == -1 && !PyErr_Occurred());
  }

};

}} // namespace pybind11::detail


int addition(int a, int b) {
  return a + b;
}

template<typename... T>
void declare_writer_impl(py::module &m, const std::string &typestr) {
  using Class = stryke::OrcWriterImpl<T...>;
  std::string pyclass_name = std::string("OrcWriterImpl_") + typestr;

  py::class_<Class>(m, pyclass_name.c_str())
    .def(py::init<>())
    ;
}

PYBIND11_MODULE(stryke, m) {

  py::class_<stryke::WriterOptions>(m, "WriterOptions")
    .def(py::init<>());

  // py::class_<stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> OrcWriterImpl_Point1i(m, "OrcWriterImpl_Point1i", py::module_local())
  //   .def(py::init<>())
  //   ;
  // declare_writer_impl<stryke::DateNumber, stryke::Int>(m, "Point1i");

  using Class = stryke::OrcWriterImpl<stryke::Int, stryke::Int>;
  std::string pyclass_name = std::string("OrcWriterImpl_") + "Point1i";

  py::class_<Class, std::shared_ptr<Class>>(m, pyclass_name.c_str())
    .def(py::init<std::array<std::string, 2>, std::string, const stryke::WriterOptions &>())
    .def("write", (void (Class::*)(stryke::Int, stryke::Int)) &Class::write);

  m.def("addition", &addition, "Make an addition", py::arg("a"), py::arg("a"));

}