This is an example on how to use [delta open sharing](https://www.databricks.com/product/delta-sharing) with [delta tables](https://docs.databricks.com/delta/index.html).

To get started as a data sender, you need:
- A [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/) enabled [Workspace](https://docs.databricks.com/data-governance/unity-catalog/enable-workspaces.html)
- A [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/) enabled [Cluster](https://docs.databricks.com/data-governance/unity-catalog/compute.html#create-clusters--sql-warehouses-with-unity-catalog-access)

To get started as a data recipient you need:
- A work enviroment with [Python](https://www.python.org/downloads/) and [PySpark](https://spark.apache.org/docs/latest/api/python/)
- The [delta-sharing Python connector](https://github.com/delta-io/delta-sharing#installation)
- A Databricks workspace (Only needed for this example but data can be loaded in a simple python enviroment as Pandas as well)


**Data Sender:**  
1. Get started by running the ['1. Setup'](https://github.com/pythoance/delta-sharing-example/blob/master/sender/1.%20Setup%20%28Sender%29.py) notebook:
   - Creates the tables used in the example
   - Creates the databricks recipient and the databricks share
   - Grants the share access to the created tables
   - Grants the recipient access to the created share

2. After the recipient is created, a description table will be displayed from where the activation_link can be retrieved. This activation link needs to be shared privately with the recipient.

3. Continue by running the ['2. Publish Data'](https://github.com/pythoance/delta-sharing-example/blob/master/sender/2.%20Publish%20Data%20%28Sender%29.py) notebook:
   - Reads the data from csv into a DataFrame
   - Appends the lines to the table that is shared

4. The ['0. Reset'](https://github.com/pythoance/delta-sharing-example/blob/master/sender/0.%20Reset%20%28Sender%29.py) notebook can then be ran to:
   - Delete the used tables and views
   - Delete the checkpoint and all extra data saved during the example
   - Re-run the setup process

**Data Recipient:**  
1. Get started by downloading the profile file from the activation link received from the data granter.

2. Continue by running the ['1. Setup'](https://github.com/pythoance/delta-sharing-example/blob/master/recipient/1.%20Setup%20%28Recipient%29.py) notebook:
   - Creates the tables used in the example

3. After that, run either the ['2.0 Read Data'](https://github.com/pythoance/delta-sharing-example/blob/master/recipient/2.0%20Read%20Data%20%28Recipient%29.py) or the ['2.1 Read Changes'](https://github.com/pythoance/delta-sharing-example/blob/master/recipient/2.1%20Read%20Changes%20%28Recipient%29.py) notebooks:
   - The first notebook reads the data in bulk. Whenever there is a data update, it overwrites the entire table.  
   - The second notebook reads only the data changes (using CDF) since the last read and applies those changes to the local table.

4. The ['0. Reset'](https://github.com/pythoance/delta-sharing-example/blob/master/recipient/0.%20Reset%20%28Recipient%29.py) notebook can then be ran to:
   - Delete the used tables and views
   - Re-run the setup process
  
The [example data](https://github.com/pythoance/delta-sharing-example/tree/master/data) was taken from the United States Bureau of Transportation.