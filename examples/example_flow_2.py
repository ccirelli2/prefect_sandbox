"""
"""
from prefect import task, Flow
import pandas as pd


@task
def capitalize(data_frame: pd.DataFrame, column_name: str):
    """ """
    data_frame[column_name] = list(
        map(lambda x: x.upper(), data_frame[column_name].values)
    )
    return data_frame


@task
def multiply(data_frame: pd.DataFrame, column_name: str, multiple: int):
    """ """
    data_frame[column_name] = list(
        map(lambda x: x * multiple, data_frame[column_name].values)
    )
    return data_frame


@task
def add_column(data_frame: pd.DataFrame, new_col_name: str):
    """ """
    data_frame[new_col_name] = ["", "", ""]
    return data_frame


if __name__ == "__main__":
    # Create DataFrame
    data_frame = pd.DataFrame({})
    data_frame["A"] = ["a", "b", "c"]
    data_frame["B"] = [1, 2, 3]

    with Flow("tutorial_2") as flow:
        # Execute code within flow
        data_frame = capitalize(data_frame, column_name="A")
        data_frame = multiply(data_frame, column_name="B", multiple=2)
        data_frame = add_column(data_frame, new_col_name="C")
        # Run Flow
    flow.run()
