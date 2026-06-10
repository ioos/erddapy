"""Griddap handling."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Hashable

import pandas as pd

from erddapy.core.url import urlopen


def _griddap_get_constraints(
    dataset_url: str,
    step: int,
) -> tuple[dict, list, list]:
    """Fetch metadata of griddap dataset and set initial constraints.

    Step size is applied to all dimensions.
    """
    dds_url = f"{dataset_url}.dds"
    res = urlopen(dds_url)
    data = res.read().decode("utf-8")
    dims, *variables = data.split("GRID")
    dim_list = dims.split("[")[:-1]
    dim_names, variable_names = [], []
    for dim in dim_list:
        dim_name = dim.split(" ")[-1]
        dim_names.append(dim_name)
    for var in variables:
        phrase, *__ = var.split("[")
        var_name = phrase.split(" ")[-1]
        variable_names.append(var_name)

    constraints_dict: dict[Hashable, str] = {}
    for dim_name in dim_names:
        url = f"{dataset_url}.csvp?{dim_name}"
        data_info = pd.read_csv(url).to_numpy()
        data_start = (
            data_info[-1][0] if dim_name == "time" else data_info[0][0]
        )

        constraints_dict[f"{dim_name}>="] = data_start
        constraints_dict[f"{dim_name}<="] = data_info[-1][0]
        constraints_dict[f"{dim_name}_step"] = str(step)

    return constraints_dict, dim_names, variable_names


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
