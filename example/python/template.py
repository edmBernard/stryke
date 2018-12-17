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
    from stryke import template as syt
except ModuleNotFoundError as e:
    print("These examples assume stryke is install on your system. ==> %s" % e)
    exit(1)


def main(batchsize, nbre_rows):
    filename = "test.orc"
    options = sy.WriterOptions()
    options.set_batch_size(batchsize)

    writer = syt.WriterDatePoint1l(["Date", "value"], filename, options)
    for i in range(nbre_rows):
        writer.write(i, i)


if __name__ == "__main__":
    main(100000, 1000)
