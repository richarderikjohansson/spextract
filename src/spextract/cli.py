import argparse
from .parse import parse_json, parse_setup, Path


def main() -> None:
    """
    TODO
    1. Default for instrument, year and month?
    2. Get a list of all input files
    3. Implement the odbc to extract data from rawdata
    4. Place data in hdf5 files
    5. Set up cron job with crontab, think of a smart way to check
    if new raw data files have been added. Only convert the new files!
    """
    setup = parse_setup()
    help = setup["help"]
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(dest="instrument", help=help["instrument"])
    parser.add_argument(dest="year", type=str, help=help["year"])
    parser.add_argument(dest="month", type=str, help=help["month"])
    parser.add_argument("--spectrometer", type=str, default="rpgffts")
    parser.add_argument("--outdir", type=str, default=None)
    args = parser.parse_args()

    assert args.instrument.lower() in setup["instruments"], (
        f"instrument: '{args.instrument}' not supported"
    )

    # set up output directory
    if args.outdir is None:
        basedir = setup["paths"]["basedir"]
        outdir = Path(basedir) / args.instrument.upper() / args.spectrometer.lower()
    else:
        outdir = Path(args.outdir)
    assert outdir.exists(), f"'{outdir}' does not exist"

    spectrometer_variables = parse_json("spectrometer")
    instrument_variables = parse_json(args.instrument)

    # set up path to input files directory
    int_month = int(args.month)
    month = f"{int_month:02}"
    year_month = f"{args.year}-{month}"
    infile_base = instrument_variables["path"]
    infiles_dir = Path(infile_base) / args.year / year_month
    print(infiles_dir)
