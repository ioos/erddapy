"""Griddap handling."""

from __future__ import annotations

import pandas as pd

from erddapy.core.url import urlopen

OptionalList = list[str] | tuple[str] | None


def _griddap_get_constraints(
    dataset_url: str,
    step: int,
) -> tuple[dict, list, list]:
    """Fetch metadata of griddap dataset and set initial constraints.

    Step size is applied to all dimensions.
    """
    dds_url = f"{dataset_url}.dds"
    url = urlopen(dds_url)
    data = url.read().decode()
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
    table = pd.DataFrame(
        {"dimension name": [], "min": [], "max": [], "length": []},
    )
    for dim in dim_names:
        url = f"{dataset_url}.csvp?{dim}"
        data = pd.read_csv(url).to_numpy()
        data_start = data[-1][0] if dim == "time" else data[0][0]

        meta = pd.DataFrame(
            [
                {
                    "dimension name": dim,
                    "min": data_start,
                    "max": data[-1][0],
                    "length": len(data),
                },
            ],
        )
        table = pd.concat([table, meta])
    table = table.set_index("dimension name", drop=True)
    constraints_dict = {}
    for dim, data in table.iterrows():
        constraints_dict[f"{dim}>="] = data["min"]
        constraints_dict[f"{dim}<="] = data["max"]
        constraints_dict[f"{dim}_step"] = step

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
    user_variables: OptionalList = None,
    original_variables: OptionalList = None,
) -> None:
    """Check user has not requested variables that do not exist in dataset."""
    invalid_variables = []
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
