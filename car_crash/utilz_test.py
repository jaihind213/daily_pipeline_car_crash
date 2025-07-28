import datetime

from car_crash.utilz import parse_damage  # noqa: E501
from car_crash.utilz import get_is_yes_no, get_output_path, was_airbag_deployed


def test_get_output_path():
    """
    Test the get_output_dir_path function.
    """
    # Test with default parameters
    assert (
        get_output_path("/tmp", datetime.date(2023, 10, 1))
        == "/tmp/year=2023/month=10/day=01"
    ), "Output directory path is not correct"


def test_parse_damage():
    """
    Test the parse_damage function.
    """
    assert parse_damage("OVER $1,500") == (
        1500,
        None,
    ), "Parsing failed for 'OVER $1,500'"
    assert parse_damage("$501 - $1,500") == (
        501,
        1500,
    ), "Parsing failed for '$501 - $1,500'"
    assert parse_damage("$501 - $1500") == (
        501,
        1500,
    ), "Parsing failed for '$501 - $1,500'"
    assert parse_damage("501 - 1500") == (
        501,
        1500,
    ), "Parsing failed for '$501 - $1,500'"
    assert parse_damage("$501  -  $1,500") == (
        501,
        1500,
    ), "Parsing failed for '$501 - $1,500'"
    assert parse_damage("$500 OR LESS") == (
        1,
        500,
    ), "Parsing failed for '$500 OR LESS'"  # noqa: E501
    assert parse_damage("$500") == (
        500,
        500,
    ), "Parsing failed for '$500 OR LESS'"  # noqa: E501
    assert parse_damage(None) == (None, None), "Parsing failed for None"
    assert parse_damage("UNKNOWN FORMAT") == (
        None,
        None,
    ), "Parsing failed for 'UNKNOWN FORMAT'"


def test_get_is_yes_no():
    """
    Test the get_is_yes_no function.
    """
    assert get_is_yes_no("Y") is True, "Parsing failed for 'Y'"
    assert get_is_yes_no("y") is True, "Parsing failed for 'Y'"
    assert get_is_yes_no("N") is False, "Parsing failed for 'N'"
    assert get_is_yes_no("YES") is True, "Parsing failed for 'YES'"
    assert get_is_yes_no("NO") is False, "Parsing failed for 'NO'"
    assert get_is_yes_no("TRUE") is True, "Parsing failed for 'TRUE'"
    assert get_is_yes_no("FALSE") is False, "Parsing failed for 'FALSE'"
    assert get_is_yes_no(" true ") is True, "Parsing failed for 'FALSE'"
    assert get_is_yes_no("1") is True, "Parsing failed for 'FALSE'"


def test_is_airbag_deployed():
    """
    Test the is_airbag_deployed function.
    """
    assert (
        was_airbag_deployed("DEPLOYED") is True
    ), "Parsing failed for 'DEPLOYED'"  # noqa: E501
    assert (
        was_airbag_deployed("DePLOYED") is True
    ), "Parsing failed for 'DEPLOYED'"  # noqa: E501
    assert (
        was_airbag_deployed("DID not DEPLOY") is False
    ), "Parsing failed for 'DID NOT DEPLOY'"
    assert (
        was_airbag_deployed("DEPLOYMENT UNKNOWN") is None
    ), "Parsing failed for 'UNKNOWN'"
    assert (
        was_airbag_deployed("NOT APPLICABLE") is None
    ), "Parsing failed for 'UNKNOWN FORMAT'"
