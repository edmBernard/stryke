//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 08/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#include "stryke_template.hpp"
#include "stryke_multifile.hpp"


using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterMulti<DateNumber, Int, Long, Double> writer({"A2", "B2", "C2","D2"}, ".", "date.orc", 10, 100);

  for (int i = 0; i < 3; ++i) {
    writer.write(1000 + i, 2000 + i+1, 3000 + i+2, 11111.111111 + i/10000.);
  }

  return 0;
}
