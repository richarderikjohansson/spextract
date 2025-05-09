from itertools import islice
from typing import Tuple
from configparser import ConfigParser
import json
import numpy as np
from .locate import Path, find_instrument_file, find_setup
try:
    import pyodbc
except ImportError:
    print("Cannot import pyodbc")


def read_sum(file: Path) -> Tuple[list, np.ndarray]:
    with open(file, "r") as data:
        head = [line.strip() for line in list(islice(data, 9))]
        spec = np.array([float(line.strip())
                        for line in list(islice(data, 1, None))])
    return head, spec


def odbc_extract():
    pass


def parse_json(name: str) -> dict:
    file_path = find_instrument_file(name)
    data = json.loads(file_path.read_bytes())

    if name == "spectrometer":
        for name, val in data.items():
            if name != "map_spectrometer":
                for key, value in val.items():
                    val[key] = int(value, 16)

    return data


def parse_setup():
    file = find_setup()
    setup = ConfigParser()
    setup.read(file)
    return setup
