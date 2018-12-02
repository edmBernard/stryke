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

#include <iostream>
#include "stryke.hpp"
#include <string>

using namespace stryke;


int main(int argc, char const *argv[])
{
    OrcWriterImpl<int, std::string> my_writer(10, 100, "my_path", "my_prefix");
    my_writer.write(1, "toto");

    return 0;
}
