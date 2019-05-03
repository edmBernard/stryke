//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 14/12/2018.
//
//  Copyright (c) 2018, 2019 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_TYPE_HPP_
#define STRYKE_TYPE_HPP_

#include "stryke/options.hpp"

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>

namespace stryke {

// ==============================================================
// Type Implementation
// ==============================================================

// All Type
// orc::BYTE        --> Not Implemented
// orc::INT
// orc::SHORT
// orc::LONG
// orc::STRING
// orc::CHAR        --> Not Implemented
// orc::VARCHAR     --> Not Implemented
// orc::BINARY      --> Not Implemented
// orc::FLOAT
// orc::DOUBLE
// orc::DECIMAL     --> Not Implemented
// orc::BOOLEAN
// orc::DATE
// orc::TIMESTAMP
// orc::STRUCT      --> Not Implemented
// orc::LIST        --> Not Implemented
// orc::MAP         --> Not Implemented
// orc::UNION       --> Not Implemented

//! stryke::Type defined an interface for type that match orc type.
//!
//! stryke::get_orc_type and stryke::utils::Filler are define outside to get stryke::Type free of orc library dependencies.
class Type {
protected:
  Type() {}
public:
  // bool empty = true;
  typedef Type type;
  static constexpr bool is_date = false;
};

//! stryke::Long defined type to match orc field type orc::TypeKind::LONG.
//!
//!
class Long : public Type {
public:
  Long(long data)
      : data(data), empty(false) {
  }
  Long() {
  }
  operator long() const {
    return data;
  }
  long data;
  bool empty = true;
  typedef Long type;
  static constexpr bool is_date = false;
};

//! stryke::Short defined type to match orc field type orc::TypeKind::SHORT.
//!
//!
class Short : public Type  {
public:
  Short(short data)
      : data(data), empty(false) {
  }
  Short() {
  }
  operator short() const {
    return data;
  }
  short data;
  bool empty = true;
  typedef Long type;
  static constexpr bool is_date = false;
};

//! stryke::Int defined type to match orc field type orc::TypeKind::INT.
//!
//!
class Int : public Type  {
public:
  Int(int data)
      : data(data), empty(false) {
  }
  Int () {
  }
  operator int() const {
    return data;
  }
  int data;
  bool empty = true;
  typedef Long type;
  static constexpr bool is_date = false;
};

//! stryke::String defined type to match orc field type orc::TypeKind::STRINGs.
//!
//!
class String : public Type  {
public:
  String(std::string &&data)
      : data(std::forward<std::string>(data)), empty(false) {
  }
  String(const std::string &data)
      : data(data), empty(false) {
  }
  String(const char *data)
      : data(std::string(data)), empty(false) {
  }
  String() {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  bool empty = true;
  typedef String type;
  static constexpr bool is_date = false;
};

//! stryke::Double defined type to match orc field type orc::TypeKind::DOUBLE.
//!
//!
class Double : public Type  {
public:
  Double(double data)
      : data(data), empty(false) {
  }
  Double() {
  }
  operator double() const {
    return data;
  }
  double data;
  bool empty = true;
  typedef Double type;
  static constexpr bool is_date = false;
};

//! stryke::Float defined type to match orc field type orc::TypeKind::FLOAT.
//!
//!
class Float : public Type  {
public:
  Float(float data)
      : data(data), empty(false) {
  }
  Float() {
  }
  operator float() const {
    return data;
  }
  float data;
  bool empty = true;
  typedef Double type;
  static constexpr bool is_date = false;
};

//! stryke::Boolean defined type to match orc field type orc::TypeKind::BOOLEAN.
//!
//!
class Boolean : public Type  {
public:
  Boolean(bool data)
      : data(data), empty(false) {
  }
  Boolean() {
  }
  operator bool() const {
    return data;
  }
  bool data;
  bool empty = true;
  typedef Boolean type;
  static constexpr bool is_date = false;
};

//! stryke::Date defined type to match orc field type orc::TypeKind::DATE.
//!
//! Build to be initialized by a std::string
class Date : public Type  {
public:
  Date(std::string &&data)
      : data(std::forward<std::string>(data)), empty(false) {
  }
  Date(const std::string &data)
      : data(data), empty(false) {
  }
  Date(const char *data)
      : data(std::string(data)), empty(false) {
  }
  Date() {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  bool empty = true;
  typedef Date type;
  static constexpr bool is_date = true;
};

//! stryke::DateNumber defined type to match orc field type orc::TypeKind::DATE.
//!
//! Build to be initialized by a long that represent number of day since 1970
class DateNumber : public Type  {
public:
  DateNumber(long data)
      : data(data), empty(false) {
  }
  DateNumber() {
  }
  operator long() const {
    return data;
  }
  long data;
  bool empty = true;
  typedef Long type;
  static constexpr bool is_date = true;
};

//! stryke::Timestamp defined type to match orc field type orc::TypeKind::TIMESTAMP.
//!
//! Build to be initialized by a std::string
class Timestamp : public Type  {
public:
  Timestamp(std::string &&data)
      : data(std::forward<std::string>(data)), empty(false) {
  }
  Timestamp(const std::string &data)
      : data(data), empty(false) {
  }
  Timestamp(const char *data)
      : data(std::string(data)), empty(false) {
  }
  Timestamp() {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  bool empty = true;
  typedef Timestamp type;
  static constexpr bool is_date = true;
};

//! stryke::Timestamp defined type to match orc field type orc::TypeKind::TIMESTAMP.
//!
//! Build to be initialized by a double that represent number of nanosecond since 1970
class TimestampNumber : public Type  {
public:
  TimestampNumber(double data)
      : data(data), empty(false) {
  }
  TimestampNumber() {
  }
  operator double() const {
    return data;
  }
  double data;
  bool empty = true;
  typedef TimestampNumber type;
  static constexpr bool is_date = true;
};

//! Simple stryke::Type wrapper to specify if a field in encode in folder or in file.
//!
//! \tparam T variadic template of all field.
//!
template <typename... T>
struct FolderEncode {
  static constexpr size_t size = sizeof...(T);
};

} // namespace stryke

#endif // !STRYKE_TYPE_HPP_
