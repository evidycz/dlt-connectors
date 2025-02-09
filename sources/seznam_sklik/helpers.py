"""Seznam Sklik source helpers"""

from dlt.common import pendulum

from .settings import SKLIK_DATA_FORMAT


def validate_and_format_dates(
    start_date: str,
    end_date: str
) -> tuple[str, str]:
    try:
        start = pendulum.parse(start_date)
        end = pendulum.parse(end_date)

        if start >= end:
            start, end = end, start

        formatted_start = start.format(SKLIK_DATA_FORMAT)
        formatted_end = end.format(SKLIK_DATA_FORMAT)

        return formatted_start, formatted_end

    except Exception as e:
        raise ValueError(f"Error processing dates: {str(e)}")
