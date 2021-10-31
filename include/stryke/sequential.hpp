//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 08/12/2018.
//
//  Copyright (c) 2018, 2019, 2020, 2021 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once

#include "date/date.h"
#include "stryke/core.hpp"
#include "stryke/dispatch.hpp"
#include "stryke/options.hpp"
#include "stryke/type.hpp"
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace stryke {

namespace utils {

template <typename T>
bool compare(T value1, T value2) {
  return value1.data == value2.data;
}

template <>
inline bool compare(TimestampNumber value1, TimestampNumber value2) {
  return long(value1.data / 86400) == long(value2.data / 86400);
}

template <>
inline bool compare(Double value1, Double value2) {
  return long(value1.data) == long(value2.data);
}

} // namespace utils

//! Writer in mutli file one thread. It close file when the trigger data change.
//!
//! \tparam T First Types that will trigger file closing. This data is encoded in folder.
//! \tparam Types Types list of field stryke::Type or stryke::FolderEncode.
//!
template <typename T, typename... Types>
class OrcWriterSequential : public OrcWriterSequential<T, FolderEncode<>, Types...> {
public:
  OrcWriterSequential(std::array<std::string, sizeof...(Types) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : OrcWriterSequential<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

//! Writer in mutli file one thread. It close file when the trigger data change.
//! I don't find a better way. I was not able to implement it with heritage from OrcWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>.
//!
//! \tparam T First Types that will trigger file closing. This data is encoded in folder.
//! \tparam TypesFolder Types list of field stryke::Type contained in a stryke::FolderEncode. These datas will be store in folder path.
//! \tparam Types Types list of field stryke::Type.
//!
template <typename T, typename... TypesFolder, typename... Types>
class OrcWriterSequential<T, FolderEncode<TypesFolder...>, Types...> {
public:
  //! Construct a new Orc Writer Sequential object.
  //!
  //! \param column_names Array/Initializer with columns name
  //! \param root_folder Output directory
  //! \param file_prefix File prefix for output file
  //! \param options Writer options
  //!
  OrcWriterSequential(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : writeroptions(options), root_folder(root_folder), file_prefix(file_prefix) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(TypesFolder) + 1);
  }

  //! Write given data to file.
  //!
  //! \param data Data stored in folder. Trigger for file closing.
  //! \param datafolder Data encoded in folder
  //! \param dataT Data stored in file
  //!
  void write(T data, TypesFolder... datafolder, Types... dataT) {
    this->write_tuple(std::make_tuple(data, datafolder...), std::make_tuple(dataT...));
  }

  //! Write given tuple to file.
  //!
  //! \param datafolder Data encoded in folder. The first element is the trigger for file closing.
  //! \param dataT Data stored in file
  //!
  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<Types...> dataT) {
    this->get_writer(std::get<0>(datafolder));
    this->writer->write_tuple(datafolder, dataT);
  }

  //! Close file and finalize it.
  //!
  //!
  void close() {
    this->writer->close();
  }

private:
  //! Get the writer object identity in function of the trigger.
  //!
  //! \param data Data stored in folder. Trigger for file closing.
  //!
  void get_writer(T data) {
    if (!utils::compare(data, this->current_data) || !this->writer) {
      this->writer = std::make_unique<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>>(this->column_names_full, this->root_folder, this->file_prefix, this->writeroptions);
      this->current_data = data;
    }
  }

  WriterOptions writeroptions; //!< Writer options
  fs::path root_folder;        //!< Output directory
  std::string file_prefix;     //!< File prefix for output file

  std::unique_ptr<OrcWriterDispatch<FolderEncode<T, TypesFolder...>, Types...>> writer;     //!< Map contain writer. The hash is compute with data stored in folder name
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names_full; //!< Array with all field name (in path and in file)
  T current_data;                                                                           //!< Value of the current data trigger
};

//! Writer in mutli file one thread. It close file when the trigger data change. Data trigger will be duplicate between folder and file.
//!
//! \tparam T Data trigger that will be duplicate between folder and file. It close file on change.
//! \tparam Types Types list of field stryke::Type or stryke::FolderEncode.
//!
template <typename T, typename... Types>
class OrcWriterSequentialDuplicate : public OrcWriterSequentialDuplicate<T, FolderEncode<>, Types...> {
public:
  OrcWriterSequentialDuplicate(std::array<std::string, sizeof...(Types) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : OrcWriterSequentialDuplicate<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

//! Writer in mutli file one thread. It close file when the trigger data change. Data trigger will be duplicate between folder and file.
//! I don't find a better way. I was not able to implement it with heritage from OrcWriterSequential<FolderEncode<T, TypesFolder...>, T, Types...>.
//!
//! \tparam T Data trigger that will be duplicate between folder and file. It close file on change.
//! \tparam TypesFolder Types list of field stryke::Type contained in a stryke::FolderEncode. These datas will be store in folder path.
//! \tparam Types Types list of field stryke::Type.
//!
template <typename T, typename... TypesFolder, typename... Types>
class OrcWriterSequentialDuplicate<T, FolderEncode<TypesFolder...>, Types...> {

  std::unique_ptr<OrcWriterSequential<T, FolderEncode<TypesFolder...>, T, Types...>> writer; //!< Map contain writer. The hash is compute with data stored in folder name
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 2> column_names_full;  //!< Array with all field name (in path and in file)

public:
  //! Construct a new Orc Writer Sequential Duplicate object.
  //!
  //! \param column_names Array/Initializer with columns name
  //! \param root_folder Output directory
  //! \param file_prefix File prefix for output file
  //! \param options Writer options
  //!
  OrcWriterSequentialDuplicate(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(TypesFolder) + 2);
    std::copy_n(column_names.begin(), 1, this->column_names_full.begin() + sizeof...(TypesFolder) + 1);

    this->writer = std::make_unique<OrcWriterSequential<T, FolderEncode<TypesFolder...>, T, Types...>>(this->column_names_full, root_folder, file_prefix, options);
  }

  //! Write given data to file.
  //!
  //! \param data Data stored in folder and in file. Trigger for file closing.
  //! \param datafolder Data encoded in folder
  //! \param dataT Data stored in file
  //!
  void write(T date, TypesFolder... datafolder, Types... dataT) {
    this->writer->write(date, datafolder..., date, dataT...);
  }

  //! Write given tuple to file.
  //!
  //! \param datafolder Data encoded in folder. The first element is the trigger for file closing.
  //! \param dataT Data stored in file
  //!
  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<T, Types...> dataT) {
    this->writer->write_tuple(datafolder, dataT);
  }

  //! Close file and finalize it.
  //!
  //!
  void close() {
    this->writer->close();
  }
};

} // namespace stryke
