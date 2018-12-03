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
#ifndef STRYKE_HPP_
#define STRYKE_HPP_

#include "orc/OrcFile.hh"
#include "orc/Type.hh"

#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <regex>
#include <string>
#include <thread>
#include <tuple>

namespace stryke {

// namespace utils {

// template <typename T, std::size_t... Indices, typename Function>
// auto for_each_impl(T &&t, std::index_sequence<Indices...>, Function &&f) -> std::vector<decltype(f(std::get<0>(t)))> {
//   return {f(std::get<Indices>(t))...};
// }

// template <typename... Types, typename Function>
// auto for_each(std::tuple<Types...> &t, Function &&f) {
//   return for_each_impl(t, std::index_sequence_for<Types...>(), f);
// }

// } // namespace utils

namespace functors {

// template <class U>
// struct executeFillValues {
//   executeFillValues(const std::vector<T> &data, orc::StructVectorBatch *batch, uint64_t numValues) : data(data), batch(batch), numValues(numValues) {}

//   const std::vector<T> &data;
//   orc::StructVectorBatch *batch;
//   uint64_t numValues;

//   template <typename T>
//   bool operator()(T &&t) {
//     fillLongValues<U, T>(this->data, this->batch, this->numValues);
//     return true;
//   }
// };

} // namespace functors

// template<typename U>
// class AddStructField
// {
//     addStructField() {}

//     template<typename T>
//     bool operator()(T &&t) {
//       fillLongValues<U, T>(this->data, this->batch, this->numValues);
//       return true;
//     }
// }


// template<typename T>
// void test<0,T>(T* array)
// {
//     return;
// }

// template<size_t Value, typename Type>
// struct AddStructField
// {
//     AddStructField<Value, Type>(std::unique_ptr<orc::Type> &struct_type) : struct_type(struct_type) {}
//     std::unique_ptr<orc::Type> &struct_type;
//     static size_t const count = 0;

//     template<typename T>
//     bool operator()(T &&t) {
//       return false;
//     }
// };

// template<size_t Value>
// struct AddStructField<Value, int> {

//   template<typename T>
//   bool operator()(T &&t) {
//     this->struct_type->addStructField("super_col" + std::to_string(this->count++), orc::createPrimitiveType(orc::TypeKind::INT));
//     return true;
//   }
// };


template<typename T>
  bool addStructField(std::unique_ptr<orc::Type> &struct_type) {
  return false;
}

template<>
bool addStructField<int>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("col_int_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::INT));
  return true;
}

template<>
bool addStructField<long>(std::unique_ptr<orc::Type> &struct_type) {
  static int count = 0;
  struct_type->addStructField("col_date_" + std::to_string(count++), orc::createPrimitiveType(orc::TypeKind::DATE));
  return true;
}

template <typename... Types>
std::vector<bool> for_each(std::unique_ptr<orc::Type> &struct_type) {
  return {addStructField<Types>(struct_type)...};
}

// template <typename... Types, std::size_t... Indices>
// std::vector<bool> for_each(std::index_sequence<Indices...>, std::unique_ptr<orc::Type> &struct_type) {
//   return {AddStructField<Indices, Types>(struct_type)()...};
// }

template <class T, uint64_t N>
void fillLongValues(const std::vector<T> &data,
                    orc::StructVectorBatch *batch,
                    uint64_t numValues) {
  orc::LongVectorBatch *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch->fields[N]);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    auto col = std::get<N>(data[i]);
    std::cout << col << " - " << std::endl;
    if (col == 0) {
      longBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      longBatch->notNull[i] = 1;
      longBatch->data[i] = col;
    }
  }
  longBatch->hasNulls = hasNull;
  longBatch->numElements = numValues;
}

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class OrcWriterImpl {
public:
  OrcWriterImpl(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix)
      : batchSize(batchSize) {

    // TODO: Trouver comment créer ce schema sans string
    fileType = orc::createStructType();
    auto ret = for_each<Types...>(fileType);

    for (auto&& i : ret) {
      std::cout << i << std::endl;
    }

    options.setStripeSize(1000);

    this->outStream = orc::writeLocalFile(folder + "/" + prefix);
    this->writer = orc::createWriter(*fileType, outStream.get(), options);
    this->rowBatch = this->writer->createRowBatch(this->batchSize);

    // this->buffer = orc::DataBuffer<char>(*orc::getDefaultPool(), 4 * 1024 * 1024);
  }

  ~OrcWriterImpl() {
    addToFile();
    std::cout << "Destrutor" << std::endl;
    this->writer->close();
  }

  void write(Types... dataT) {

    // std::cout << "argument length: " << sizeof...(data) << std::endl;
    if (this->numValues < this->batchSize) {

      this->data.push_back(std::make_tuple(dataT...));
      ++this->numValues;

    } else {
      addToFile();
    }
  }

  void addToFile() {
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(this->rowBatch.get());
    structBatch->numElements = this->numValues;
    {
      // auto ret = utils::for_each(this->parsers, functors::executeFillValues(this->data, structBatch, this->numValues));
      // for (auto &&i : ret) {
      //   std::cout << i << " " << std::endl;
      // }

      fillLongValues<std::tuple<Types...>, 0>(this->data, structBatch, this->numValues);
      fillLongValues<std::tuple<Types...>, 1>(this->data, structBatch, this->numValues);
      fillLongValues<std::tuple<Types...>, 2>(this->data, structBatch, this->numValues);

    }

    this->writer->add(*this->rowBatch);

    this->data.clear();
    this->numValues = 0;
  }

  OrcWriterImpl &setSchema() {
    std::cout << "Apply a schema" << std::endl;
    return *this;
  }

private:
  std::unique_ptr<orc::Type> fileType;
  orc::WriterOptions options;
  std::unique_ptr<orc::OutputStream> outStream;
  std::unique_ptr<orc::Writer> writer;
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch;

  std::vector<std::tuple<Types...>> data; // buffer that holds a batch of rows in tuple

  uint64_t numValues = 0; // num of lines read in a batch
  uint64_t batchSize;
};

//! Writer in mutli file one thread.
//!
//!
template <typename... Types>
class OrcWriterMulti {
public:
  OrcWriterMulti(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix) {
  }

  void write(Types... data) {
    this->fifo.push(data...);
  }
};

//! Writer in mutli file separate thread.
//!
//!
template <typename... Types>
class OrcWriter {
public:
  OrcWriter(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix) {
  }

  void write(Types... data) {
    this->fifo.push(data...);
  }

  std::tuple<Types...> get() {
    return this->fifo.pop();
  }

  std::queue<std::tuple<Types...>> fifo;
  std::thread writer_thread;
};

} // namespace stryke

#endif // !STRYKE_HPP_
