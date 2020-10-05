"""
Documentation helpers

"""


def show_iframe(src):
    """Helper function to show HTML returns."""
    from IPython.display import IFrame

    return IFrame(src=src, width="100%", height=950)
