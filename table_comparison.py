"""
table_comparison.py
~~~~~~~~~~~~~~~~~~~
"""
import os
import pandas as pd
import threading
import multiprocessing
from pprint import pprint
from itertools import combinations
from collections import OrderedDict
from typing import List, Dict, Any, Tuple, OrderedDict

# Number of CPUs
CPUS = multiprocessing.cpu_count()
print('Number of cpus == {}'.format(CPUS))


class DataTable(object):
    """Loads in a database table"""

    def __init__(self):
        self.data_frame = None

    def load_data_frame(self, full_file_path: str, sheet_name=None) -> None:
        """Loads the file into a Pandas DataFrame
        Attributes
        ----------
        full_file_path: str
            The fully qualified file path and file name
        sheet_name: str
            The optional sheet name for loading an excel file into a Pandas DataFrame
        """
        try:
            if os.path.isfile(full_file_path):
                file_extension = os.path.splitext(full_file_path)
                if file_extension[-1] == '.xlsx':
                    # Load the excel file into a Pandas DataFrame
                    self.data_frame = pd.read_excel(full_file_path, sheet_name=sheet_name)
                elif file_extension[-1] == '.csv':
                    # Load the csv file into a Pandas DataFrame
                    self.data_frame = pd.read_csv(full_file_path)
            else:
                raise OSError('File: {}, could not be found'.format(full_file_path))
        except Exception as e:
            print(e)


class TableComparison(object):
    """Class to perform table comparisons"""

    def __init__(self):
        self.master_dict: Dict[str, Any] = {}

    def run_table_comparison(self, *file_names) -> None:
        """Perform tables comparisons on a given set of files
        Attributes
        ----------
        file_names: str
            This is a dynamically sized variable that can take any number of fully qualified file
            paths in to perform

        Returns
        -------
        None
        """
        try:
            # Create a tuple of all file combinations for comparison
            table_comparisons = list(combinations(file_names, 2))

            # Run a comparison on each tuple from table_combinations
            for t in table_comparisons:
                # Create instances of the DataTable class
                df_one = DataTable()
                df_two = DataTable()
                file_extension_t0 = os.path.splitext(t[0])
                file_extension_t1 = os.path.splitext(t[1])
                print(file_extension_t1)

                # Both files are excel files
                if file_extension_t0[-1] == '.xlsx' and file_extension_t1[-1] == '.xlsx':
                    # Load in the data sets
                    df_one.load_data_frame(full_file_path=t[0], sheet_name='Sheet1')
                    df_two.load_data_frame(full_file_path=t[1], sheet_name='Sheet1')
                # File 1 is excel and file 2 is a csv file
                elif file_extension_t0[-1] == '.xlsx' and file_extension_t1[-1] == '.csv':
                    df_one.load_data_frame(full_file_path=t[0], sheet_name='Sheet1')
                    df_two.load_data_frame(full_file_path=t[1])
                # File 1 is a csv file and File 2 is an excel file
                elif file_extension_t0[-1] == '.csv' and file_extension_t1[-1] == '.xlsx':
                    df_one.load_data_frame(full_file_path=t[0])
                    df_two.load_data_frame(full_file_path=t[1], sheet_name='Sheet1')
                # Both files are csv files
                elif file_extension_t0[-1] == '.csv' and file_extension_t1[-1] == '.csv':
                    df_one.load_data_frame(full_file_path=t[0])
                    df_two.load_data_frame(full_file_path=t[1])

                # Create a temporary dictionary that contains a Pandas Series that is comprised
                #   of True/False values of the current table's column by column comparisons
                temp_dict = {'{}_vs_{}'.format(list(df_one.data_frame.columns)[j], list(df_two.data_frame.columns)[j])
                             : df_one.data_frame[list(df_one.data_frame.columns)[j]].values
                               == df_two.data_frame[list(df_two.data_frame.columns)[j]].values
                             for j, _ in enumerate(list(df_one.data_frame.columns))}

                # true_counts stores the following:
                # 1 - The total number of rows of the Table
                # 2 - The count of identical rows between tables
                # 3 - The percent difference between the two tables
                true_counts = {}
                for k in temp_dict.keys():
                    temp_list = list(temp_dict[k])
                    true_list = []
                    for i in temp_list:
                        if i == True:
                            true_list.append(i)
                    # Update the true_counts dictionary
                    true_counts[k] = {'Number of Rows': len(temp_list),
                                      'Number of Identical Rows': len(true_list),
                                      'Percent of Identical Rows Between Tables(%)': float(len(true_list) / len(temp_list))}

                # Insert the true_counts dictionary into the master level dictionary
                self.master_dict['{}_vs_{}'.format(os.path.basename(t[0]), os.path.basename(t[1]))] = true_counts



        except Exception as e:
            print(e)







def main():
    # Files for testing
    file_one = r'C:\Users\moder\OneDrive\Desktop\sample_testing_xlsx.xlsx'
    file_two = r'C:\Users\moder\OneDrive\Desktop\sample_testing_xlsx_copy.xlsx'
    file_three = r'C:\Users\moder\OneDrive\Desktop\sample_testing_diff_copy.xlsx'


    # Data table creation
    dt_one = DataTable()
    dt_one.load_data_frame(full_file_path=file_one, sheet_name='Sheet1')

    # Table comparisons
    tbl_comparisons = TableComparison()
    tbl_comparisons.run_table_comparison(file_one, file_two, file_three)
    pprint(tbl_comparisons.master_dict)


if __name__ == '__main__':
    main()
