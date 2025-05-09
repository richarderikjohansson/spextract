from pathlib import Path


def find_instrument_file(instrument_file: str) -> Path:
    cwd = Path(__file__)
    basedir = "instrument"

    for parent in cwd.parents:
        current = parent / basedir
        if current.exists():
            for file in current.iterdir():
                if instrument_file.lower() in file.name:
                    return file
    raise FileNotFoundError(f"'{instrument_file}' file not found")


def find_setup() -> Path:
    cwd = Path(__file__)
    setup = "setup.ini"
    for parent in cwd.parents:
        current = parent / setup
        if current.exists():
            return current
    raise FileNotFoundError("Can not locate {setup}")
