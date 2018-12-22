
import python.stryke as sy
import pytest
from path import Path
from datetime import datetime


@pytest.mark.parametrize("batchsize, nbre_rows", [
    (10, 10),
    (100, 100),
    (1000, 1000),
    (10000, 10000)
])
def test_batch(batchsize, nbre_rows):
    filename = "test.orc"
    options = sy.WriterOptions()
    options.set_batch_size(batchsize)

    writer = sy.simple.WriterTimestampNPoint1l(["Date", "value"], filename, options)
    for i in range(nbre_rows):
        writer.write(i, -i)

    del writer

    assert Path(filename).exists()

    # TODO: add test check
    # stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    # REQUIRE(tmp_b.nbr_columns == 3);
    # REQUIRE(tmp_b.nbr_rows == nbr_rows);


@pytest.mark.parametrize("batchsize, nbre_rows", [
    (10, 10),
    (100, 100),
    (1000, 1000),
    (10000, 10000)
])
def test_lock_file(batchsize, nbre_rows):
    filename = "test.orc"
    options = sy.WriterOptions()
    options.set_batch_size(batchsize)

    writer = sy.simple.WriterDateNPoint1l(["Date", "value"], filename, options)
    for i in range(nbre_rows):
        writer.write(i, i)

    assert Path(filename).exists()
    assert Path(filename + ".lock").exists()

    del writer

    assert Path(filename).exists()
    assert not Path(filename + ".lock").exists()


