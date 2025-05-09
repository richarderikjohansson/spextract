from datetime import datetime, timedelta
import requests
from requests import Response


def get_temperature(dt: datetime) -> float:
    """Function to get the temperature

    Args:
        dt: date time of temperature reading

    Returns:
        Temperature in Celsius
    """
    if dt.hour == 0:
        shift_flag = True
    else:
        shift_flag = False

    target = make_target(dt, shift_flag)
    response = perform_request(target)
    dictionary = clean_content(response)

    # shift dt to match content from response
    new_dt = dt - timedelta(hours=1)
    min_key = min(dictionary.keys(), key=lambda k: abs(k - new_dt))
    return dictionary[min_key]


def clean_content(response: Response) -> dict:
    """Function to clean the content from the response

    Args:
        response: Response object from request

    Returns:
        Cleaned content
    """
    content = response.content.decode("utf-8").split("\n")
    dictionary = dict()

    # make checks that fields contain the data needed
    for element in content:
        sub = element.split()
        if len(sub) > 1 and sub[0] != "x" and sub[1] != "x":
            dt = datetime.strptime(sub[0], "%Y%m%d%H%M%S")
            dictionary[dt] = float(sub[1])

    return dictionary


def make_target(datetime_object: datetime, shift_day: bool) -> str:
    """Function to construct target for request

    Args:
        datetime_object: Date and time for when temperature is wanted
        shift_day: Flag to shift day, only in edge cases

    Returns:
        Target URL
    """
    if shift_day:
        new_dt = datetime_object - timedelta(hours=1)
        year = new_dt.year
        month = new_dt.month
        day = new_dt.day
    else:
        year = datetime_object.year
        month = datetime_object.month
        day = datetime_object.day

    target = f"https://www2.irf.se/weather/get_day_ascii.php?year={
        year}&month={month}&day={day}"
    return target


def perform_request(target: str) -> Response:
    """Function to do the request

    Args:
        target: Target URL

    Returns:
        Response object
    """
    r = requests.get(target)
    if r.status_code != 200:
        print(r, "can not connect to dc")
    else:
        response = requests.get(url=target)
        return response
