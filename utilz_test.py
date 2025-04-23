import datetime

from utilz import get_output_path


def test_get_output_path():
    """
    Test the get_output_dir_path function.
    """
    # Test with default parameters
    assert (
        get_output_path("/tmp", datetime.date(2023, 10, 1))
        == "/tmp/year=2023/month=10/day=01"
    ), "Output directory path is not correct"
