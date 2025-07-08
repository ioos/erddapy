"""Documentation helpers."""


def show_iframe(src):
    """Show HTML returns on Jupyter outputs."""
    from IPython.display import IFrame  # noqa: PLC0415

    return IFrame(src=src, width="100%", height=950)
