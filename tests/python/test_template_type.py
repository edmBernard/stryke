
import python.stryke as sy
import pytest
from path import Path
from datetime import datetime


@pytest.fixture()
def orc_file():
    filename = "test.orc"
    yield filename
    Path(filename).remove()


@pytest.fixture()
def orc_options():
    yield sy.WriterOptions()


def test_WriterLong1(orc_file, orc_options):
    writer = sy.simple.WriterLong1(["col1"], orc_file, orc_options)
    data_input = [
        1,
        "1",
        "+1",
        "+1 ",
        -1,
        "-1",
        "-1 "
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerLong1(orc_file)

    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == int(i)


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterLong1_special_character(orc_file, orc_options):
    writer = sy.simple.WriterLong1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Double1(orc_file, orc_options):
    writer = sy.simple.WriterDouble1(["col1"], orc_file, orc_options)
    data_input = [
        1,
        -1,
        1.0,
        -1.0,
        0.123456,
        -0.123456,
        "1",
        "-1",
        "1.0",
        "-1.0",
        "0.123456",
        "-0.123456"
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerDouble1(orc_file)
    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == float(i)


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterDouble1_special_character(orc_file, orc_options):
    writer = sy.simple.WriterDouble1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Boolean1(orc_file, orc_options):
    writer = sy.simple.WriterBoolean1(["col1"], orc_file, orc_options)
    data_input = [
        1,
        0,
        True,
        False,
        []
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerBoolean1(orc_file)
    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == bool(i)


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterBoolean1_special_character(orc_file, orc_options):
    writer = sy.simple.WriterDouble1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Date1(orc_file, orc_options):
    writer = sy.simple.WriterDate1(["col1"], orc_file, orc_options)
    data_input = [
        "2018-12-10",
        "2018-12-10 12:12:12",
        "2018-12-10 12:12:12.123456789",
        datetime.now(),
        str(datetime.now())
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerDate1(orc_file)
    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == str(i)[:10]


def test_DateN1(orc_file, orc_options):
    writer = sy.simple.WriterDateN1(["col1"], orc_file, orc_options)
    data_input = [
        1,
        datetime.now().timestamp() / (60*60*24)
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerDateN1(orc_file)
    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == int(i)


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_DateN1_string(orc_file, orc_options):
    writer = sy.simple.WriterDateN1(["col1"], orc_file, orc_options)
    writer.write("2018-12-10")
    del writer
    assert Path(orc_file).exists()


def test_Timestamp1(orc_file, orc_options):
    writer = sy.simple.WriterTimestamp1(["col1"], orc_file, orc_options)
    data_input = [
        (1, "1970-01-01 00:00:00"),
        (1.123456789, "1970-01-01 00:00:00.123456789"),
        ("2018-12-10", "2018-12-10 00:00:00"),
        ("2018-12-10 12:12:12", "2018-12-10 12:12:12"),
        ("2018-12-10 12:12:12.123456789", "2018-12-10 12:12:12.123456789")
    ]

    for i in data_input:
        writer.write(i[0])

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerTimestamp1(orc_file)

    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == i[1]


def test_TimestampN1(orc_file, orc_options):
    writer = sy.simple.WriterTimestampN1(["col1"], orc_file, orc_options)
    data_input = [
        1,
        1.123456789,
        datetime.now().timestamp()
    ]

    for i in data_input:
        writer.write(i)

    del writer
    assert Path(orc_file).exists()

    data = sy.simple.readerTimestampN1(orc_file)
    assert len(data) == len(data_input)

    for d, i in zip(data, data_input):
        assert d[0] == float(i)


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_TimestampN1_string(orc_file, orc_options):
    writer = sy.simple.WriterTimestampN1(["col1"], orc_file, orc_options)
    writer.write("2018-12-10 12:12:12.123456789")
    del writer
    assert Path(orc_file).exists()
