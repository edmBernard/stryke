//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once
#ifndef STRYKE_CORE_HPP_
#define STRYKE_CORE_HPP_

#include "date/date.h"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "stryke/options.hpp"
#include "stryke/type.hpp"


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

namespace fs = std::filesystem;
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

// We replace TypeKind method in stryke::Type by an external get_orc_type to get stryke::Type without orc dependencies
template <typename Type>
inline orc::TypeKind const get_orc_type();

template <>
inline orc::TypeKind const get_orc_type<stryke::Long>() {
  return orc::TypeKind::LONG;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Short>() {
  return orc::TypeKind::SHORT;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Int>() {
  return orc::TypeKind::INT;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::String>() {
  return orc::TypeKind::STRING;
}

// template <>
// inline orc::TypeKind const get_orc_type<stryke::Char>() {
//   return orc::TypeKind::CHAR;
// }

template <>
inline orc::TypeKind const get_orc_type<stryke::Double>() {
  return orc::TypeKind::DOUBLE;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Float>() {
  return orc::TypeKind::FLOAT;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Boolean>() {
  return orc::TypeKind::BOOLEAN;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Date>() {
  return orc::TypeKind::DATE;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::DateNumber>() {
  return orc::TypeKind::DATE;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::Timestamp>() {
  return orc::TypeKind::TIMESTAMP;
}

template <>
inline orc::TypeKind const get_orc_type<stryke::TimestampNumber>() {
  return orc::TypeKind::TIMESTAMP;
}

namespace utils {

// ==============================================================
// Filler Implementation: function to fill batch
// ==============================================================

// I use a class because function template partial specialisation is not allowed in c++, but class yes
template <typename Types, uint64_t N, typename T>
class Filler {
public:
  // This function is commented to throw on exception at compile time if one of requested type is not implemented
  // static bool fillValue(const std::vector<Types> &data,
  //                       orc::StructVectorBatch *batch,
  //                       uint64_t numValues) {
  //   return false;
  // }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Long> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        longBatch->notNull[i] = 1;
        longBatch->data[i] = col.data;
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, String> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::StringVectorBatch *stringBatch = dynamic_cast<orc::StringVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        if (string_buffer->size() - string_buffer_offset < col.size()) {
          string_buffer->reserve(string_buffer->size() * 2);
        }
        memcpy(string_buffer->data() + string_buffer_offset,
               col.c_str(),
               col.size());
        stringBatch->data[i] = string_buffer->data() + string_buffer_offset;
        stringBatch->length[i] = static_cast<int64_t>(col.size());
        string_buffer_offset += col.size();
      }
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Double> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::DoubleVectorBatch *dblBatch = dynamic_cast<orc::DoubleVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        dblBatch->notNull[i] = 1;
        dblBatch->data[i] = col.data;
      }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Boolean> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *boolBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        boolBatch->notNull[i] = 1;
        boolBatch->data[i] = col.data;
      }
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Date> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {

        std::istringstream inputStream{col};
        date::sys_seconds tp;
        inputStream >> date::parse("%F", tp);

        batch->notNull[i] = 1;
        longBatch->data[i] = std::chrono::duration_cast<date::days>(tp.time_since_epoch()).count();
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, DateNumber> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        longBatch->data[i] = int64_t(col.data);
      }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, Timestamp> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      std::string col = std::get<N>(data[i]).data;
      if (col.empty()) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        date::sys_seconds tp;
        std::istringstream inputStream{col};
        inputStream >> date::parse("%F %T", tp);

        if (inputStream.fail()) {
          inputStream.clear();
          inputStream.seekg(0, inputStream.beg);
          inputStream >> date::parse("%F", tp);
        }

        tsBatch->data[i] = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
        char *tail;
        const char *left = std::strpbrk(col.c_str(), ".");
        if (left != nullptr) {
          double d = strtod(left, &tail);
          if (tail != left) {
            tsBatch->nanoseconds[i] = static_cast<int64_t>(d * 1000000000.0);
          } else {
            tsBatch->nanoseconds[i] = 0;
          }
        } else {
          tsBatch->nanoseconds[i] = 0;
        }
      }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
    return true;
  }
};

template <typename Types, uint64_t N>
class Filler<Types, N, TimestampNumber> {
public:
  static bool fillValue(const std::vector<Types> &data,
                        orc::StructVectorBatch *batch,
                        uint64_t numValues,
                        std::unique_ptr<orc::DataBuffer<char>> &string_buffer,
                        uint64_t &string_buffer_offset) {
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch->fields[N]);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
      auto col = std::get<N>(data[i]);
      if (col.empty) {
        batch->notNull[i] = 0;
        hasNull = true;
      } else {
        batch->notNull[i] = 1;
        double decimale, integrale;
        decimale = std::modf(col.data, &integrale);
        if (decimale < 0) {
          decimale += 1;
          integrale -= 1;
        }
        tsBatch->data[i] = int64_t(integrale);
        if (decimale > 0) {
          tsBatch->nanoseconds[i] = static_cast<int64_t>(decimale * 1000000000.0);
        } else {
          tsBatch->nanoseconds[i] = 0;
        }
      }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
    return true;
  }
};

template <typename T, std::size_t... Indices>
auto fillValuesImpl(std::index_sequence<Indices...>, std::vector<T> &data, orc::StructVectorBatch *structBatch, uint64_t numValues, std::unique_ptr<orc::DataBuffer<char>> &string_buffer, uint64_t &string_buffer_offset) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {Filler<T, Indices, typename std::tuple_element<Indices, T>::type::type>::fillValue(data, structBatch, numValues, string_buffer, string_buffer_offset)...};
}

template <typename... Types>
auto fillValues(std::vector<std::tuple<Types...>> &data, orc::StructVectorBatch *structBatch, uint64_t numValues, std::unique_ptr<orc::DataBuffer<char>> &string_buffer, uint64_t &string_buffer_offset) {
  return fillValuesImpl(std::index_sequence_for<Types...>(), data, structBatch, numValues, string_buffer, string_buffer_offset);
}

// ==============================================================
//  Create Schema Implementation: function to create data schema
// ==============================================================

template <typename T>
bool addStructField(std::unique_ptr<orc::Type> &struct_type, std::string column_name) {
  struct_type->addStructField(column_name, orc::createPrimitiveType(get_orc_type<T>()));
  return true;
}

template <typename... Types, std::size_t... Indices>
auto createSchemaImpl(std::index_sequence<Indices...>, std::unique_ptr<orc::Type> &struct_type, std::array<std::string, sizeof...(Types)> column_names) -> std::vector<bool> {
  // return {Filler<T, Indices, decltype(std::get<Indices>(data[0]))>::fillValue(data, structBatch, numValues)...};
  return {addStructField<Types>(struct_type, column_names[Indices])...};
}

template <typename... Types>
auto createSchema(std::unique_ptr<orc::Type> &struct_type, std::array<std::string, sizeof...(Types)> column_names) {
  return createSchemaImpl<Types...>(std::index_sequence_for<Types...>(), struct_type, column_names);
}

} // namespace utils

// ==============================================================
//  Simple Writer Implementation
// ==============================================================

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class OrcWriterImpl {
public:
  OrcWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string root_folder, std::string filename, const WriterOptions &options)
      : writeroptions(options), column_names(column_names), root_folder(root_folder), filename(filename) {
    this->fileType = orc::createStructType();
    auto ret = utils::createSchema<Types...>(this->fileType, this->column_names);

    this->orc_writeroptions.setStripeSize(writeroptions.stripeSize);
    fs::path folder_path = (this->root_folder / this->filename).parent_path();
    if (!folder_path.empty()) {
      fs::create_directories(folder_path);
    }
    this->outStream = orc::writeLocalFile(this->root_folder / this->filename);
    this->writer = orc::createWriter(*this->fileType, outStream.get(), this->orc_writeroptions);
    this->rowBatch = this->writer->createRowBatch(this->writeroptions.batchSize);

    this->string_buffer = std::make_unique<orc::DataBuffer<char>>(*orc::getDefaultPool(), 4 * 1024 * 1024); // Create buffer for string data. One buffer store all string data

    if (this->writeroptions.create_lock_file) {
      std::ofstream outfile(this->root_folder / (this->filename + ".lock"));
      outfile.close();
    }
  }

  OrcWriterImpl(std::array<std::string, sizeof...(Types)> column_names, std::string filename, const WriterOptions &options)
      : OrcWriterImpl(column_names, "", filename, options) {
  }

  ~OrcWriterImpl() {
    addToFile();
    this->writer->close();
    if (this->writeroptions.create_lock_file) {
      std::filesystem::remove(this->filename + ".lock");
    }
  }

  void write(Types... dataT) {
    this->write_tuple(std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<Types...> dataT) {

    if (this->numValues >= this->writeroptions.batchSize) {
      addToFile();
    }

    this->data.push_back(dataT);
    ++this->numValues;
  }

  void addToFile() {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(this->rowBatch.get());
    structBatch->numElements = this->numValues;

    auto ret = utils::fillValues(this->data, structBatch, this->numValues, this->string_buffer, this->string_buffer_offset);

    this->writer->add(*this->rowBatch);

    this->data.clear();
    this->numValues = 0;
    this->string_buffer_offset = 0;
  }

private:

  std::unique_ptr<orc::Type> fileType;

  WriterOptions writeroptions;
  orc::WriterOptions orc_writeroptions;

  std::unique_ptr<orc::OutputStream> outStream;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch;

  std::vector<std::tuple<Types...>> data; // buffer that holds a batch of rows in tuple
  std::array<std::string, sizeof...(Types)> column_names;

  std::unique_ptr<orc::DataBuffer<char>> string_buffer;
  uint64_t string_buffer_offset = 0;

  fs::path root_folder;
  std::string filename;

  uint64_t numValues = 0; // num of lines read in a batch
};

} // namespace stryke

#endif // !STRYKE_CORE_HPP_
