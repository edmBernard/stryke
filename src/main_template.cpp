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

#include "stryke_template.hpp"
#include <iostream>
#include <string>

using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterImpl<Int, Short, Long, Double, Float> writer_number({"A1", "B1", "C1","D1", "E1"}, 10, 100, ".", "number.orc");
  OrcWriterImpl<Int, DateNumber, TimestampNumber, Date, Timestamp> writer_date({"A2", "B2", "C2","D2", "E2"}, 10, 100, ".", "date.orc");

  for (int i = 0; i < 3; ++i) {

    writer_number.write(1000 + i, 2000 + i+1, 3000 + i+2, 11111.111111 + i/10000.,222222.2222222 + i/10000.);
    writer_date.write(1000 + i, 2000 + i+1, 222222.2222222 + i/10000., std::string("1990-12-18 12:26:20"), std::string("1980-12-18 12:26:20.1234567"));

  }

  return 0;
}
