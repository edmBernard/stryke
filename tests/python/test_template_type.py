
import python.stryke as sy
from python.stryke import template as syt
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
    writer = syt.WriterLong1(["col1"], orc_file, orc_options)
    writer.write(1)
    writer.write("1")
    writer.write("+1")
    writer.write("+1 ")
    writer.write(-1)
    writer.write("-1")
    writer.write("-1 ")
    del writer
    assert Path(orc_file).exists()


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterLong1_special_character(orc_file, orc_options):
    writer = syt.WriterLong1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Double1(orc_file, orc_options):
    writer = syt.WriterDouble1(["col1"], orc_file, orc_options)
    writer.write(1)
    writer.write(-1)
    writer.write(1.0)
    writer.write(-1.0)
    writer.write(0.123456)
    writer.write(-0.123456)
    writer.write("1")
    writer.write("-1")
    writer.write("1.0")
    writer.write("-1.0")
    writer.write("0.123456")
    writer.write("-0.123456")
    del writer
    assert Path(orc_file).exists()


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterDouble1_special_character(orc_file, orc_options):
    writer = syt.WriterDouble1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Boolean1(orc_file, orc_options):
    writer = syt.WriterBoolean1(["col1"], orc_file, orc_options)
    writer.write(1)
    writer.write(0)
    writer.write(True)
    writer.write(False)
    del writer
    assert Path(orc_file).exists()


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_WriterBoolean1_special_character(orc_file, orc_options):
    writer = syt.WriterDouble1(["col1"], orc_file, orc_options)
    writer.write("$")
    del writer
    assert Path(orc_file).exists()


def test_Date1(orc_file, orc_options):
    writer = syt.WriterDate1(["col1"], orc_file, orc_options)
    writer.write("2018-12-10")
    writer.write("2018-12-10 12:12:12")
    writer.write("2018-12-10 12:12:12.123456789")
    writer.write(datetime.now())
    writer.write(str(datetime.now()))
    del writer
    assert Path(orc_file).exists()


def test_DateN1(orc_file, orc_options):
    writer = syt.WriterDateN1(["col1"], orc_file, orc_options)
    writer.write(1)
    writer.write(datetime.now().timestamp() / (60*60*24))
    del writer
    assert Path(orc_file).exists()


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_DateN1_string(orc_file, orc_options):
    writer = syt.WriterDateN1(["col1"], orc_file, orc_options)
    writer.write("2018-12-10")
    del writer
    assert Path(orc_file).exists()


def test_Timestamp1(orc_file, orc_options):
    writer = syt.WriterTimestamp1(["col1"], orc_file, orc_options)
    writer.write(1)  # write null value
    writer.write(1.123456789)  # write null value
    writer.write("2018-12-10")  # write null value
    writer.write("2018-12-10 12:12:12")
    writer.write("2018-12-10 12:12:12.123456789")
    writer.write(datetime.now())
    del writer
    assert Path(orc_file).exists()


def test_TimestampN1(orc_file, orc_options):
    writer = syt.WriterTimestampN1(["col1"], orc_file, orc_options)
    writer.write(1)
    writer.write(1.123456789)
    writer.write(datetime.now().timestamp())
    del writer
    assert Path(orc_file).exists()


@pytest.mark.xfail(strict=True, raises=TypeError)
def test_TimestampN1_string(orc_file, orc_options):
    writer = syt.WriterTimestampN1(["col1"], orc_file, orc_options)
    writer.write("2018-12-10 12:12:12.123456789")
    del writer
    assert Path(orc_file).exists()
