//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 14/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
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

// Long Type Category
class Long {
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

class Short {
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

class Int {
public:
  Int(int data)
      : data(data), empty(false) {
  }
  Int() {
  }
  operator int() const {
    return data;
  }
  int data;
  bool empty = true;
  typedef Long type;
  static constexpr bool is_date = false;
};

// String Type Category
class String {
public:
  String(std::string &&data)
      : data(std::forward<std::string>(data)) {
  }
  String(const std::string &data)
      : data(data) {
  }
  String(const char *data)
      : data(std::string(data)) {
  }
  String() {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  typedef String type;
  static constexpr bool is_date = false;
};

// Not working
// class Char {
// public:
//   Char(char data)
//       : data(1, data) {
//   }
//   Char() {
//   }
//   std::string data;
//   typedef String type;
// };

// Double Type Category
class Double {
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

class Float {
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

// Boolean Type Category
class Boolean {
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

// Date Type Category
class Date {
public:
  Date(std::string &&data)
      : data(std::forward<std::string>(data)) {
  }
  Date(const std::string &data)
      : data(data) {
  }
  Date(const char *data)
      : data(std::string(data)) {
  }
  Date() {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  typedef Date type;
  static constexpr bool is_date = true;
};

class DateNumber {
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

// Timestamp Type Category
class Timestamp {
public:
  Timestamp(std::string &&data)
      : data(std::forward<std::string>(data)) {
  }
  Timestamp(const std::string &data)
      : data(data) {
  }
  Timestamp() {
  }
  Timestamp(const char *data)
      : data(std::string(data)) {
  }
  operator std::string() const {
    return data;
  }
  std::string data;
  typedef Timestamp type;
  static constexpr bool is_date = true;
};

class TimestampNumber {
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

template <typename... T>
struct FolderEncode {
  static constexpr size_t size = sizeof...(T);
};

} // namespace stryke

#endif // !STRYKE_TYPE_HPP_
