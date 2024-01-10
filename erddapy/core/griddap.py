"""Griddap handling."""

import functools

import pandas as pd

from erddapy.core.url import urlopen

ListLike = list[str] | tuple[str]


@functools.lru_cache(maxsize=128)
def _griddap_get_constraints(
    dataset_url: str,
    step: int,
) -> tuple[dict, list, list]:
    """
    Fetch metadata of griddap dataset and set initial constraints.

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
    table = pd.DataFrame({"dimension name": [], "min": [], "max": [], "length": []})
    for dim in dim_names:
        url = f"{dataset_url}.csvp?{dim}"
        data = pd.read_csv(url).values
        if dim == "time":
            data_start = data[-1][0]
        else:
            data_start = data[0][0]

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


def _griddap_check_constraints(user_constraints: dict, original_constraints: dict):
    """Check that constraints changed by user match those expected by dataset."""
    if user_constraints.keys() != original_constraints.keys():
        raise ValueError(
            "keys in e.constraints have changed. Re-run e.griddap_initialize",
        )


def _griddap_check_variables(user_variables: ListLike, original_variables: ListLike):
    """Check user has not requested variables that do not exist in dataset."""
    invalid_variables = []
    for variable in user_variables:
        if variable not in original_variables:
            invalid_variables.append(variable)
    if invalid_variables:
        raise ValueError(
            f"variables {invalid_variables} are not present in dataset. Re-run e.griddap_initialize",
        )
