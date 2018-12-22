#
#  Stryke
#
#  https://github.com/edmBernard/Stryke
#
#  Created by Erwan BERNARD on 17/12/2018.
#
#  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
#  Distributed under the Apache License, Version 2.0. (See accompanying
#  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
#


try:
    # These examples assume stryke is install on your system
    import stryke as sy
except ModuleNotFoundError as e:
    print("These examples assume stryke is install on your system. ==> %s" % e)
    exit(1)

import argparse
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write a Orc file')
    parser.add_argument('--filename', '-f', type=str, required=True, help='The name of the file')
    parser.add_argument('--batch_size', type=int, default=10000, help='The batch size')
    parser.add_argument('--nbr_row', type=int, default=100, help='The batch size')

    args = parser.parse_args()

    options = sy.WriterOptions()
    options.set_batch_size(args.batch_size)

    writer = sy.simple.WriterTimestampPoint2l(["Date", "x", "y"], args.filename, options)
    for i in range(args.nbr_row):
        writer.write(datetime.now(), i, i * 2)

