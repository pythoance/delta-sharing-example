import math

import delta_sharing
import pandas as pd
import numpy as np

# The path to the profile file that you got from the share granter.
profile_file = "config.share"
client = delta_sharing.SharingClient(profile_file)

global_data_frame = pd.DataFrame({
    'origin': pd.Series(dtype='str'),
    'amount': pd.Series(dtype='int'),
    'latest_version': pd.Series(dtype='int')},
)


def show_tables():
    """
    Display all tables that are available with the given profile file.
    """
    print("Tables:")
    for share in client.list_shares():
        for schema in client.list_schemas(share):
            for table in client.list_tables(schema):
                print(f"  - {share.name}.{schema.name}.{table.name}")


def get_max(df: pd.DataFrame, column_name: str, default_value=0):
    """
    Get the max value from a column from a DataFrame. If no value is found, the default_value is used.
    :param df: The dataframe in which to look.
    :param column_name: The name of the column in which to look.
    :param default_value: The default value in case no value is found.
    :return: The highest value from a column from a table.
    """
    max_value = df[column_name].max()
    if max_value is None or math.isnan(max_value):
        return int(default_value)
    return int(max_value)


def read_changes(table_url, starting_version: int = 0):
    """
    Read the changes that have been made to a remote shared table and put them in a local pandas DataFrame but exclude:
     - versions older than the latest recorded version in our local table. the newest recorded version will be reprocessed for continuity
     - update_preimage changes because they just show the previous value in case of a row update
     - old changes of a row if there are multiple because only the latest change matters
    :param table_url: The url of the remote table for which to read the changes.
    :param starting_version: The version from where to start reading changes. Defaults to 0.
    :return: The latest changes for each row as a pandas DataFrame
    """

    # Read Changes and rename columns.
    df = delta_sharing.load_table_changes_as_pandas(url=table_url, starting_version=starting_version) \
        .rename(columns={"_commit_version": "commit_version",
                         "_commit_timestamp": "commit_timestamp",
                         "_change_type": "change_type"})

    # Remove rows with the change type update_preimage as they are not needed.
    df = df[df['change_type'] != "update_preimage"]

    # Rename the update_postimage change type to update.
    df["change_type"] = np.where(df["change_type"] == "update_postimage", "update", df["change_type"])

    # Rank changes so that the newest changes for a row have the lowest rank.
    df['rank'] = df.groupby("origin")["commit_version"].rank(ascending=False)

    # Return only the latest version of the changes for each row and drop the ranking column.
    return df[df['rank'] == 1].drop(["rank"], axis=1)


def apply_changes(source: pd.DataFrame, target: pd.DataFrame):
    """
    Apply the changes of a table into a local delta table.
    :param source: The DataFrame that contains the changes.
    :param target: The DataFrame where the changes will be applied.
    :return: The updated target DataFrame.
    """

    def delete(data_frame: pd.DataFrame, row_to_drop: pd.Series):
        return data_frame[data_frame["origin"] != row_to_drop["origin"]]

    def insert(data_frame: pd.DataFrame, row_to_insert: pd.Series):
        to_insert = pd.DataFrame(
            data={"origin": row_to_insert["origin"],
                  "amount": row_to_insert["amount"],
                  "latest_version": row_to_insert["commit_version"]},
            index=[data_frame.size])
        return pd.concat([data_frame, to_insert])

    def update(data_frame: pd.DataFrame, row_to_update: pd.Series):
        data_frame.loc[data_frame["origin"] == row_to_update["origin"], "amount"] = row["amount"]
        data_frame.loc[data_frame["origin"] == row_to_update["origin"], "latest_update"] = row["commit_version"]

        return data_frame

    source.reset_index()
    for index, row in source.iterrows():
        match row["change_type"]:
            case "delete":
                target = delete(target, row)
            case "insert":
                if row["origin"] in target["origin"].values:
                    target = update(target, row)
                else:
                    target = insert(target, row)
            case "update":
                if row["origin"] in target["origin"].values:
                    target = update(target, row)
                else:
                    target = insert(target, row)

    return target


def start():
    global global_data_frame
    table_url = profile_file + "#example_share.default.example_table"

    latest_version = get_max(global_data_frame, "latest_version")
    print("latest version: " + str(latest_version))

    changes = read_changes(table_url, latest_version)
    print()
    print("changes: ")
    print(changes.to_string())

    global_data_frame = apply_changes(changes, global_data_frame)
    print()
    print("final data:")
    print(global_data_frame)


if __name__ == '__main__':
    start()
