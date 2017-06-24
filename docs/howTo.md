# Tutorial

This document is about how to use the python scripts provided to extract useful information from raw data files.

## Prepare raw data files
Before run the scripts, you need to make sure that your data is under the directory `/vismooc-test-data/elearning-data/`. Each course is a sub-directory of `/vismooc-test-data/elearning-data/`, and inside each course directory, there are two folds which are `eventData` and `databaseData`. All log files of the course will be in `eventData`, and other database file should locate in `databaseData` directory.

For example, if you have one course, namely `course1`, the structure of your file tree should like this:
<pre>
/
|   vismooc-test-data
|   |----elearning-data
|   |    |----course1
|   |    |    |----databaseData
|   |    |    |    |----file1
|   |    |    |    |----file2
|   |    |    |    |----more files...
|   |    |    |----eventData
|   |    |    |    |----logfile1
|   |    |    |    |----logfile2
|   |    |    |    |----more log files...
</pre>

If you have some coursewares which do not want to be processed, you can add `.ignore` suffix to the courseware directory to ignore them.

For the naming convention and data structure of each file, please refer to [Edx Research Guide](http://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/index.html).

## Run the script
Our project is built with `python 3.5`, so before you run the script, make sure that `python 3.5` and `pip` installed on your machine. Besides, the scripts will dump all the data into `mongodb` when finishing the extracting procedure, so `mongodb` is also needed in your local machine with port `27017`(which is also the default port) and  **without** username and password.

After set up the environment. you may run the script with the following command:

1. Change the working directory to the project root.
2. run `pip install -r requirements.txt` to install all dependencies.
3. run `python main_old.py`.

When the scripts finished, you may find the processed data in your mongodb within `testVismoocElearning` collection.
