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

#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <tuple>

namespace stryke {

//! Writer in one file one thread.
//!
//!
template <typename... Types>
class OrcWriterImpl {
public:
  OrcWriterImpl(uint64_t batchSize, int batchNb_max, std::string folder, std::string prefix) : batchSize(batchSize) {

    // TODO: Trouver comment créer ce schema sans string
    fileType = orc::Type::buildTypeFromString("struct<x:int>");

    options.setStripeSize(1000);

    outStream = orc::writeLocalFile(folder + "/" + prefix);
    writer = orc::createWriter(*fileType, outStream.get(), options);
    rowBatch = writer->createRowBatch(this->batchSize);

  }

  ~OrcWriterImpl() {
    this->writer->close();
  }

  void write(Types... data) {

    std::cout << "argument length: " << sizeof...(data) << std::endl;
    if (this->numValues < this->batchSize) {

      this->data.push_back(std::make_tuple(data...));
      ++this->numValues;

    } else {
      // TODO: Faire le remplissage via template
      // TODO: on fait le remplissage à chaque fin de batch ou à chaque ajout de data ?

      orc::StructVectorBatch* structBatch = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
      structBatch->numElements = numValues;
    }

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
  uint64_t numValues = 0;
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
