#!/usr/bin/env python3

#
# This is a replacement program for SpExtract.exe (which only runs under win/wine)
#
# History: v0.1 first try
#          v0.2 activated "--outfile" parameter
#          v0.3 split into "reading from dbf" and "writing to sum file", i.e. support for matlab
#          v0.4 added --jsonin & --jsonout to transfer raw data over stdin/stdout
#               added possible usage of ssh to run spextract on an remote (odbc) server
#               activated parameter "--extractmode"
#
# Bugs/ToDo: * dbase record-no is not visible to odbc
#            * Only the latest versions (20091126, 20101014, 20110705 and 20151106)
#              of .dbf files are supported for now !!!
#            * produce some logging output
#            * split-version (over ssh) is very unreliable, crashes on any error on the remote side
#
# requirements: python3, pyodbc, numpy, parallel-ssh
#               "unixodbc",
#               The licensed "Devart ODBC driver for xBase" (https://www.devart.com/odbc/xbase)
#

import os
import sys
import pyodbc
import numpy
import getopt
import json
from pssh.clients import SSHClient


def print_version():
    print("spextract.py v0.4c JG/KIT 2021-01-25")


def print_usage():
    print_version()
    print(
        " Extracts all or just the last total integrated spectrum from a dBase file into a .sum file"
    )
    print(
        " Usage: $ spextract.py  -s <spectrometer>                   choose the right spectrometer"
    )
    print("                       --spectrometer=<spectrometer> ")
    print(
        "                        -i <inputfile>                      which dBase file to use"
    )
    print("                       --infile=<inputfile> ")
    print(
        "                      [ -o <outputpath>]                    output directory"
    )
    print("                      [--outdir=<outputpath>] ")
    print(
        "                        -f <outputfile>                     name of the output file"
    )
    print("                       --outfile=<outputfile> ")
    print(
        "                        -x <mode>                           choose one of the extraction modes"
    )
    print("                       --extractmode=<mode> ")
    print(
        "                      [ -h]                                 prints this help text"
    )
    print("                      [--help] ")
    print(
        "                      [--host=<remote-ssh-host>]            hostname/address of an ssh remote host"
    )
    print(
        "                      [--port=<remote-ssh-port>]            port number of the ssh remote host"
    )
    print(
        "                      [--user=<remote-ssh-user>]            username at the ssh remote host"
    )
    print(
        "                      [--jsonout]                           write data to stdout in json format"
    )
    print(
        "                      [--jsonin]                            read data in json format from stdin"
    )
    print(
        "                      [ -v]                                 returns the version of this program"
    )
    print("                      [--version] ")
    print(" <spectrometer> must be aos, ffts, rpgffts, rpgffts2 or offts")
    print(
        " <mode> must be 1 (for all spectra) or 3 (for just the last spectrum in the database)"
    )
    print(
        ' "ssh mode" is activated when using --host and then it\'s mandatory to also --name'
    )
    print(" Remark: * Any existing output file will be overridden.")


# def test():
#  s1 = (1.33, 10.234, 23.133)
#  measdt = numpy.dtype([('date', '<U10'), ('noOfChannels', '<i4'), ('spectrum', '<f16', (3,))])
#  meas = numpy.array(('2010-01-01',2048,s1), dtype=measdt)
#  meas10 = { 'date': '2010-01-01', 'noOfChannels': 2048, 'spectrum':(numpy.float128(1.33), numpy.float128(10.234), 23.133) }
#  meas11 = { 'date': '2010-01-01', 'noOfChannels': 2048, 'spectrum':(numpy.float64(1.3333333333333333333333), numpy.float32(10.2344444444444444444444444), 23.1335555555555555555555555) }
#  meas12 = { 'date': '2010-01-01', 'noOfChannels': 2048, 'spectrum':(numpy.float64(1.3333333333333333333333), numpy.float64(10.2344444444444444444444444), numpy.float64(23.1335555555555555555555555)) }
#  return meas12


def import_odbc(infile: str, spectrometer: str, xmode: int):
    """contact the odbc service to get spectra from dBase files"""
    # test for high precision support
    fi = numpy.finfo(numpy.longdouble)
    assert fi.nmant == 63 and fi.nexp == 15, "80-bit float support is required"
    del fi

    db_driver = "{Devart ODBC Driver for xBase}"

    # do some checks if all files/ dirs are available
    if not os.path.isfile(infile):
        print(" Error: input file not found.")
        return  # 20
    database = os.path.dirname(
        os.path.abspath(infile)
    )  # '/home/vu3254/data/campaigns/Kiruna2012/mirror-mira2pc/f/data/measure/spectra/2020/2020-01'
    db_file = os.path.basename(infile)  # 'Data_2020-01-03_13-22-46.DBF'
    (db_table, db_type) = os.path.splitext(
        db_file
    )  # ( 'Data_2020-01-03_13-22-46' , '.DBF' )
    if db_type.lower() != ".dbf":
        print(" Error: input-database is not dBase?")
        return  # 30

    connection = pyodbc.connect(
        "driver=" + db_driver + ";"
        "database=" + database + ";"
        "DBFFormat=Auto;"
        "CodePage=Default;"
        "ConnectMode=Shared"
    )
    cursor = connection.cursor()

    # for row in cursor.columns(table=db_table):
    #  print(row.column_name)

    # following definitions are copied from CSpectrumManagement.h as of 2020-01-17

    # aosmode
    qaosmode = {
        "NONE": 0x00,  # set w/o valid spectrum (e.g. env. only)
        "AOS_ZERO": 0x01,  # AOS set to null-termination
        "AOS_COMB": 0x02,  # AOS set to frequency-calibration
        "AOS_SIGNAL": 0x04,  # AOS set to frontend
        "FFTS_SIGNAL": 0x08,  # FFTS set to frontend
        "FFTS_ZERO": 0x10,  # FFTS set to null-termination
        "RPGFFTS_SIGNAL": 0x20,  # RPGFFTS set to frontend
        "RPGFFTS2_SIGNAL": 0x21,  # RPGFFTS set to frontend, backend2
        "RPGFFTS_ZERO": 0x40,  # RPGFFTS set to null-termination
        "RPGFFTS2_ZERO": 0x41,  # RPGFFTS set to null-termination, be2
        "OFFTS_ZERO": 0x80,  # OmnisysFFTS set to null-termination
        "OFFTS_SIGNAL": 0x81,
    }  # OmnisysFFTS set to frontend

    # femode
    qfrontend = {
        "SIGNAL": 0x01,  # frontend watches the sky
        "HOTLOAD": 0x02,  # frontend points to hot load
        "COLDLOAD": 0x03,  # frontend points to cold load
        "MIXLOAD": 0x04,  # frontend points to mix load
        "COLDEXT": 0x05,  # frontend points to external cold load
        "FLASHL": 0x06,  # frontend points to 'flashlight'
        "TREC": 0x10,
    }  # spectrum contains receiver's noise-temp.

    # unit
    qdataunits = {
        "COUNTS": 0x01,  # spectrum is in counts of spectrometer
        "K": 0x02,  # spectrum is in Kelvin
        "HZ": 0x04,
    }  # spectrum is in Hertz

    # misc
    qmisc = {
        "TOTALINT": 0x01,  # record keeps total-integrated spectrum
        "PARTINT": 0x02,  # record keeps part-integrated spectrum
        "ACCUOVRFLW": 0x04,  # data faulty due to accumulator-overflow
        "ADCOVRFLW": 0x08,
    }  # data faulty due to a/d-conv.-overflow

    # gas-type
    #  qgas = { 'CLO'  : 0x01,
    #           'O3SB' : 0x02,
    #           'O3'   : 0x03,
    #           'HNO3' : 0x04,
    #           'N2O'  : 0x05,
    #           'O3DL' : 0x06 }

    spectrometer = spectrometer.upper()
    if spectrometer == "AOS":
        qraosmode = qaosmode["AOS_SIGNAL"]
    elif spectrometer == "FFTS":
        qraosmode = qaosmode["FFTS_SIGNAL"]
    elif spectrometer == "RPGFFTS":
        qraosmode = qaosmode["RPGFFTS_SIGNAL"]
    elif spectrometer == "RPGFFTS2":
        qraosmode = qaosmode["RPGFFTS2_SIGNAL"]
    elif spectrometer == "OFFTS":
        qraosmode = qaosmode["OFFTS_SIGNAL"]
    else:
        print(" Error: spectrometer type is unset or unknown.")
        return  # 10

    # îf we have to extract only the last (i.e. latest) spectrum, we
    # *sort the query result by id using descending order and
    # *limit the query to one record only
    if xmode == 3:
        sort_order = "DESC"
        limit = "LIMIT 1"
    else:
        sort_order = "ASC"
        limit = ""

    sql = (
        "SELECT date,time,spectrum,integrate1,integrate2,integrate_ms,elevation,azimuth,gas,noOfChannels,version "
        "FROM '%s' WHERE aosmode=%d AND femode=%d AND unit=%d AND misc=%d ORDER BY id %s %s"
        % (
            db_table,
            qraosmode,
            qfrontend["SIGNAL"],
            qdataunits["K"],
            qmisc["TOTALINT"],
            sort_order,
            limit,
        )
    )
    # print('Query: \"%s\"' % sql)
    cursor.execute(sql)

    nmeas = list(dict())

    # We do not have 'float' types in python that use exactly 80 bits (8 bytes) in memory.
    # Therefore we use numpys larger "longdouble" and pad the rest of the mantissa with zeroes.
    len_float80 = 10  # no way to extract this from dtype/finfo, info comes from CSpectrumManagement::SaveRecord()
    # len_padded=numpy.dtype(numpy.longdouble).itemsize

    # iterate through all the records that were found
    for qr in cursor:
        meas = dict()
        try:
            meas["source"] = infile
            # allocate empty space (all zeros) for spectrum in longdouble format
            n = numpy.zeros(qr.noOfChannels, dtype=numpy.longdouble)
            # spectrum in double format (for matlab)
            nlist = []
            for i in range(qr.noOfChannels):
                for j in range(len_float80):
                    (n.data[i: i + 1].cast("B"))[j] = qr.spectrum[
                        (i * len_float80) + j
                    ]
                nlist.append(numpy.float64(n[i]))

            # create / fill the measurement-dictionary 'meas'
            meas["date"] = qr.date.strftime("%d.%m.%Y")
            meas["time"] = qr.time
            meas["gas"] = qr.gas
            meas["integration"] = qr.integrate1 * qr.integrate2
            meas["integrationtime"] = qr.integrate_ms
            meas["elevation"] = float(qr.elevation)
            meas["azimuth"] = float(qr.azimuth)
            meas["spectrometer"] = spectrometer
            meas["spectrum"] = nlist
        except TypeError:
            print("Error extracting selected spectrum from dbase table.")
            # fill the measurement-dictionary 'meas' with zeros
            meas["date"] = "01.01.1970"
            meas["time"] = 0
            meas["gas"] = "none"
            meas["integration"] = 0
            meas["integrationtime"] = 0
            meas["elevation"] = 0.0
            meas["azimuth"] = 0.0
            meas["spectrometer"] = "none"
            meas["spectrum"] = []

        nmeas.append(meas)

    # print("found {} records.".format(len(nmeas)))
    return nmeas


def import_ssh(host, port, username, infilename, spectrometer, xmode):
    # remote_spextract='/home/vu3254/data/python/spextract/spextract.py'
    # remote_spextract='python3 /home/gross-j/bin/spextract.py'
    remote_spextract = "./bin/spextract.py"
    cmd = "{} --jsonout --infile={} --spectrometer={} --extractmode={}".format(
        remote_spextract, infilename, spectrometer, xmode
    )
    # cmd='{} --outdir=/home/vu3254/temp --outfile=test.sum --infile={} --spectrometer={} > /home/vu3254/debug.out'.format(remote_spextract,infilename,spectrometer)
    # cmd='{} --version'.format(remote_spextract)
    # cmd='set'
    # print(cmd)

    client = SSHClient(host, username)
    host_out = client.run_command(cmd)
    # print("remote cmd executed.")

    jsondata = bytes()
    meas = dict()

    try:
        while not host_out.channel.eof():
            no, str = host_out.channel.read(1000)
            jsondata += str
    except:
        print("XXXXXXXXXXXX")

    # print("read {} bytes.".format(len(jsondata)))
    # print("data: {}".format(jsondata.decode("utf-8")))
    meas = json.loads(jsondata.decode("utf-8"))
    return meas


def export_files(nmeas, infile, outdir, outfile_org):
    fileno = 1
    for meas in nmeas:
        # automatic creation of filename for output file?
        if outfile_org == "":
            db_file = os.path.basename(
                meas["source"]
            )  # 'Data_2020-01-03_13-22-46.DBF'
            (db_table, db_type) = os.path.splitext(
                db_file
            )  # ( 'Data_2020-01-03_13-22-46' , '.DBF' )
            outfile = "%s_%s.SUM" % (db_table, meas["spectrometer"])
        else:
            outfile = outfile_org

        # insert record-no into filename if more than one file will be written
        if len(nmeas) > 1:
            (outfile_name, outfile_type) = os.path.splitext(outfile)
            outfile = "%s_%05d%s" % (outfile_name, fileno, outfile_type)

        # resulting output directory available?
        if not os.path.isdir(os.path.dirname(os.path.join(outdir, outfile))):
            print(" Error: cannot access output directory.")
            return 40

        if len(meas["spectrum"]) > 0:
            qrstartCh = 0
            qrstopCh = len(meas["spectrum"]) - 1

            # now open the output file for writing:
            try:
                outf = open(os.path.join(outdir, outfile),
                            mode="w", newline="\r\n")
                outf.write("date: %s  %s\n" % (meas["date"], meas["time"]))
                outf.write("gas: %s\n" % meas["gas"])
                outf.write("integration: %d samples\n" % meas["integration"])
                outf.write("integration-time: %d ms\n" %
                           meas["integrationtime"])
                outf.write("elevation: %.2f deg\n" % meas["elevation"])
                outf.write("azimuth: %.2f deg\n" % meas["azimuth"])
                outf.write("spectrometer: %s\n" % meas["spectrometer"])
                outf.write("source: %s\n" % meas["source"])
                outf.write(
                    "record-no: -1\n"
                )  # sorry, no easy way to get the REAL dbase record number...
                outf.write(
                    "data from channel %d to %d follows\n" % (
                        qrstartCh, qrstopCh)
                )
                for ch in range(qrstartCh, qrstopCh + 1):
                    outf.write("%27.19f\n" % meas["spectrum"][ch])
            except:
                print(" Error: cannot write to %s" %
                      os.path.join(outdir, outfile))
                return 60
            finally:
                outf.close()

        fileno += 1


def export_json(nmeas):
    sys.stdout.buffer.write(json.dumps(nmeas).encode("utf-8"))


def import_json():
    nmeas = list(dict())
    nmeas = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    return nmeas


# called from cmdline (i.e. not imported)
if __name__ == "__main__":
    # some default values, change this for your needs
    spectrometer = "unkown"
    infilename = ""
    outdir = "."
    outfilename = ""
    xmode = 0
    jsonin = False
    jsonout = False
    host = ""
    port = int(22)
    username = ""
    use_ssh = False

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "s:i:o:f:x:hv",
            [
                "spectrometer=",
                "infile=",
                "outdir=",
                "outfile=",
                "extractmode=",
                "help",
                "version",
                "jsonin",
                "jsonout",
                "host=",
                "port=",
                "user=",
                "ssh",
            ],
        )
    except getopt.GetoptError as err1:
        print(str(err1))  # a message from getopt
        sys.exit(1)
    for o, a in opts:
        if o in ("-s", "--spectrometer"):
            spectrometer = a
        elif o in ("-i", "--infile"):
            infilename = a
        elif o in ("-o", "--outdir"):
            outdir = a
        elif o in ("-f", "--outfile"):
            outfilename = a
        elif o in ("-x", "--extractmode"):
            xmode = int(a)
        elif o in ("-h", "--help"):
            print_usage()
            sys.exit(0)
        elif o in ("-v", "--version"):
            print_version()
            sys.exit(0)
        elif o in ("--jsonin"):
            jsonin = True
        elif o in ("--jsonout"):
            jsonout = True
        elif o in ("--host"):
            host = a
            use_ssh = True
        elif o in ("--port"):
            port = int(a)
        elif o in ("--user"):
            username = a

    if jsonin and jsonout:
        print("Error: Either import from OR export to JSON. Not both.")
        sys.exit(20)

    if (xmode != 1) and (xmode != 3) and (import_json == False):
        print("Error: Parameter --extractmode missing. See --help.")
        sys.exit(30)

    # start extraction now
    nmeas = list(dict())
    if jsonin:
        nmeas = import_json()
    else:
        if use_ssh:
            nmeas = import_ssh(host, port, username,
                               infilename, spectrometer, xmode)
        else:
            nmeas = import_odbc(infilename, spectrometer, xmode)

    if jsonout:
        sys.exit(export_json(nmeas))
    else:
        sys.exit(export_files(nmeas, infilename, outdir, outfilename))
