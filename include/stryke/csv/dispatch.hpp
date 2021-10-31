//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 03/04/2019.
//
//  Copyright (c) 2018, 2019, 2020, 2021 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//
#pragma once

#include "date/date.h"
#include "stryke/csv/core.hpp"
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
namespace csv {

namespace utils {

template <typename T>
std::chrono::time_point<std::chrono::system_clock> get_time(const T &date);

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<Date>(const Date &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<DateNumber>(const DateNumber &date) {
  std::chrono::time_point<std::chrono::system_clock> p1;
  return p1 + std::chrono::seconds(date.data * (60 * 60 * 24));
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<Timestamp>(const Timestamp &date) {
  std::istringstream inputStream{date.data};
  date::sys_seconds tp;
  inputStream >> date::parse("%F %T", tp);
  return tp;
}

template <>
inline std::chrono::time_point<std::chrono::system_clock> get_time<TimestampNumber>(const TimestampNumber &date) {
  std::chrono::time_point<std::chrono::system_clock> p1;
  return p1 + std::chrono::seconds(long(date.data));
}


inline std::array<std::string, 3> tp2ymd(std::chrono::time_point<std::chrono::system_clock> tp) {
  // Process Date
  auto ymd = date::year_month_day(date::floor<date::days>(tp)); // calendar date
  char month_buffer[12];                                        // for padding
  char day_buffer[12];                                          // for padding
  sprintf(day_buffer, "%.02d", unsigned(ymd.day()));
  sprintf(month_buffer, "%.02d", unsigned(ymd.month()));
  auto y = std::to_string(int(ymd.year()));
  return {y, month_buffer, day_buffer};
}

template <typename T>
bool addFolder(T value, std::string column_name, fs::path &folder) {
  if constexpr (T::is_date) {
    auto ymd = tp2ymd(get_time(value));
    folder /= "year=" + std::get<0>(ymd);
    folder /= "month=" + std::get<1>(ymd);
    folder /= "day=" + std::get<2>(ymd);
  } else {
    std::ostringstream oss;
    oss << column_name << "=" << value.data;
    folder /= oss.str();
  }
  return true;
};


template <typename... Types, std::size_t... Indices>
auto createFolderImpl(std::index_sequence<Indices...>, std::tuple<Types...> const &data, std::array<std::string, sizeof...(Types)> column_names, fs::path &folder) -> std::vector<bool> {
  return {addFolder(std::get<Indices>(data), std::get<Indices>(column_names), folder)...};
}

template <typename... Types>
auto createFolder(std::tuple<Types...> const &data, std::array<std::string, sizeof...(Types)> column_names, fs::path &folder) {
  return createFolderImpl(std::index_sequence_for<Types...>(), data, column_names, folder);
}

template <typename T>
bool addFileSuffix(T value, std::string &suffix) {
  if constexpr (T::is_date) {
    auto ymd = tp2ymd(get_time(value));
    suffix += "-" + std::get<0>(ymd) + "-" + std::get<1>(ymd) + "-"  + std::get<2>(ymd);
  } else {
    std::ostringstream oss;
    oss << "-" << value.data;
    suffix += oss.str();
  }
  return true;
}


template <typename... Types, std::size_t... Indices>
auto createFileSuffixImpl(std::index_sequence<Indices...>, std::tuple<Types...> const &data, std::string &suffix) -> std::vector<bool> {
  return {addFileSuffix(std::get<Indices>(data), suffix)...};
}

template <typename... Types>
auto createFileSuffix(std::tuple<Types...> const &data, std::string &suffix) {
  return createFileSuffixImpl(std::index_sequence_for<Types...>(), data, suffix);
}

} // namespace utils

namespace fs = std::filesystem;

template <typename... Types>
class CsvWriterDispatch : public CsvWriterDispatch<FolderEncode<>, Types...> {
public:
  CsvWriterDispatch(std::array<std::string, sizeof...(Types)> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : CsvWriterDispatch<FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

//! Writer in mutli file one thread.
//!
//!
template <typename... TypesFolder, typename... Types>
class CsvWriterDispatch<FolderEncode<TypesFolder...>, Types...> {
public:
  CsvWriterDispatch(std::array<std::string, sizeof...(TypesFolder) + sizeof...(Types)> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : writeroptions(options), root_folder(root_folder), file_prefix(file_prefix) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder), this->column_names_folder.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder), sizeof...(Types), this->column_names_file.begin());
  }

  ~CsvWriterDispatch() {
  }

  void write(TypesFolder... datafolder, Types... dataT) {
    this->write_tuple(std::make_tuple(datafolder...), std::make_tuple(dataT...));
  }

  void write_tuple(std::tuple<TypesFolder...> datafolder, std::tuple<Types...> dataT) {
    std::string writers_path = this->get_writer(datafolder);
    this->writers[writers_path]->write_tuple(dataT);
  }

  void close() {
    this->writers.clear();
    this->counts.clear();
  }

protected:
  std::string get_writer(std::tuple<TypesFolder...> datafolder) {

    // Create folder with column data
    fs::path datafolder_path;
    utils::createFolder(datafolder, this->column_names_folder, datafolder_path);

    // Get prefix_with_date
    std::string datafolder_filesuffix;
    utils::createFileSuffix(datafolder, datafolder_filesuffix);

    std::string prefix_with_date = this->file_prefix + datafolder_filesuffix;

    // Get folder with date
    fs::path file_folder = this->root_folder / datafolder_path;

    if (this->writers.count(prefix_with_date) == 0 || (this->writeroptions.nbr_batch_max > 0 && this->writers[prefix_with_date]->get_count() >= this->writeroptions.batchSize * this->writeroptions.nbr_batch_max)) {

      if (this->writers.count(prefix_with_date) == 0) {
        this->counts[prefix_with_date] = -1;
      }

      fs::path filename;
      do {
        if (this->writeroptions.suffix_timestamp) {
          filename = file_folder / (prefix_with_date + "-" + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()) + ".csv");
        } else {
          filename = file_folder / (prefix_with_date + "-" + std::to_string(++this->counts[prefix_with_date]) + ".csv");
        }

      } while (fs::exists(filename));

      fs::create_directories(filename.parent_path());

      this->writers[prefix_with_date] = std::make_unique<CsvWriterImpl<Types...>>(this->column_names_file, filename.string(), this->writeroptions);
    }
    return prefix_with_date;
  }

  WriterOptions writeroptions;
  std::array<std::string, sizeof...(Types)> column_names_file;
  std::array<std::string, sizeof...(TypesFolder)> column_names_folder;

  std::map<fs::path, std::unique_ptr<CsvWriterImpl<Types...>>> writers;
  std::map<fs::path, int> counts; // counts for suffix in filename

  fs::path root_folder;
  std::string file_prefix;
};

template <typename T, typename... Types>
class CsvWriterDispatchDuplicate : public CsvWriterDispatchDuplicate<T, FolderEncode<>, Types...> {
public:
  CsvWriterDispatchDuplicate(std::array<std::string, sizeof...(Types) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options)
      : CsvWriterDispatchDuplicate<T, FolderEncode<>, Types...>(column_names, root_folder, file_prefix, options) {
  }
};

// I don't find a better way. I was not able to implement it with heritage from CsvWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>.
template <typename T, typename... TypesFolder, typename... Types>
class CsvWriterDispatchDuplicate<T, FolderEncode<TypesFolder...>, Types...> {

  std::unique_ptr<CsvWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>> writer;
  std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 2> column_names_full;

public:
  CsvWriterDispatchDuplicate(std::array<std::string, sizeof...(Types) + sizeof...(TypesFolder) + 1> column_names, std::filesystem::path root_folder, std::string file_prefix, const WriterOptions &options) {
    std::copy_n(column_names.begin(), sizeof...(TypesFolder) + 1, this->column_names_full.begin());
    std::copy_n(column_names.begin() + sizeof...(TypesFolder) + 1, sizeof...(Types), this->column_names_full.begin() + sizeof...(TypesFolder) + 2);
    std::copy_n(column_names.begin(), 1, this->column_names_full.begin() + sizeof...(TypesFolder) + 1);

    this->writer = std::make_unique<CsvWriterDispatch<FolderEncode<T, TypesFolder...>, T, Types...>>(this->column_names_full, root_folder, file_prefix, options);
  }

  void write(T date, TypesFolder... datafolder, Types... dataT) {
    this->writer->write(date, datafolder..., date, dataT...);
  }

  void write_tuple(std::tuple<T, TypesFolder...> datafolder, std::tuple<T, Types...> dataT) {
    this->writer->write_tuple(datafolder, dataT);
  }

  void close() {
    this->writer->close();
  }
};

} // namespace csv
} // namespace stryke
