"""Griddap handling."""

import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Hashable

from erddapy.core.url import urlopen


def _griddap_get_constraints(
    dataset_url: str,
    step: int,
) -> tuple[dict, list, list]:
    """Fetch metadata of griddap dataset and set initial constraints.

    Step size is applied to all dimensions.
    """
    xmlns = "https://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"

    ncml_url = f"{dataset_url}.ncml"
    res = urlopen(ncml_url)
    xml = res.read().decode("utf-8")
    root = ET.fromstring(xml)  # noqa: S314

    variables = root.findall(f"{{{xmlns}}}variable")
    dimensions = root.findall(f"{{{xmlns}}}dimension")

    dimension_names = [dimension.attrib["name"] for dimension in dimensions]
    variable_names = [variable.attrib["name"] for variable in variables]
    variable_names = list(set(variable_names).difference(dimension_names))

    constraints_dict: dict[Hashable, str] = {}

    ns = {"nc": "https://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"}

    for dimension_name in dimension_names:
        element = root.find(
            path=f'nc:variable[@name="{dimension_name}"]/nc:attribute[@name="actual_range"]',
            namespaces=ns,
        )
        if element is None:
            msg = (
                f"Could not find actual_range for dimension {dimension_name}."
            )
            raise ValueError(msg)
        actual_range = element.attrib["value"]
        range_min, range_max = actual_range.split()
        if dimension_name == "time":
            range_min = range_max

        constraints_dict[f"{dimension_name}>="] = range_min
        constraints_dict[f"{dimension_name}<="] = range_max
        constraints_dict[f"{dimension_name}_step"] = str(step)

    return constraints_dict, dimension_names, variable_names


def _griddap_check_constraints(
    user_constraints: dict,
    original_constraints: dict,
) -> None:
    """Validate user constraints against the dataset."""
    if user_constraints.keys() != original_constraints.keys():
        msg = (
            "keys in e.constraints have changed. Re-run e.griddap_initialize",
        )
        raise ValueError(msg)


def _griddap_check_variables(
    user_variables: list[str] | tuple[str],
    original_variables: list[str] | tuple[str],
) -> None:
    """Check user has not requested variables that do not exist in dataset."""
    invalid_variables: list[str] = []
    invalid_variables.extend(
        variable
        for variable in user_variables
        if variable not in original_variables
    )
    if invalid_variables:
        msg = (
            f"variables {invalid_variables} are not present in dataset. "
            "Re-run e.griddap_initialize"
        )
        raise ValueError(msg)
