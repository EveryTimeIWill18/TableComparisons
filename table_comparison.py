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
from typing import List, Dict, Any, Tuple

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

    def run_table_comparison(self, sheet_names=None, *file_names) -> None:
        """Perform tables comparisons on a given set of files
        Attributes
        ----------
        file_names: str
            This is a dynamically sized variable that can take any number of fully qualified file
            paths in to perform

        sheet_names: dict
            A dictionary of sheet names with the key being the file name
        Returns
        -------
        None
        """
        try:
            # Create a tuple of all file combinations for comparison
            table_comparisons = list(combinations(file_names, 2))
            print(table_comparisons)

            # Run a comparison on each tuple from table_combinations
            for t in table_comparisons:
                # Create instances of the DataTable class
                df_one = DataTable()
                df_two = DataTable()

                # Grab the file extension for each file
                file_extension_t0 = os.path.splitext(t[0])
                file_extension_t1 = os.path.splitext(t[1])

                # Both files are excel files
                if file_extension_t0[-1] == '.xlsx' and file_extension_t1[-1] == '.xlsx':
                    # Load in the data sets
                    df_one.load_data_frame(full_file_path=t[0], sheet_name=sheet_names[os.path.basename(t[0])])
                    df_two.load_data_frame(full_file_path=t[1], sheet_name=sheet_names[os.path.basename(t[1])])
                # File 1 is excel and file 2 is a csv file
                elif file_extension_t0[-1] == '.xlsx' and file_extension_t1[-1] == '.csv':
                    df_one.load_data_frame(full_file_path=t[0], sheet_name=sheet_names[os.path.basename(t[0])])
                    df_two.load_data_frame(full_file_path=t[1])
                # File 1 is a csv file and File 2 is an excel file
                elif file_extension_t0[-1] == '.csv' and file_extension_t1[-1] == '.xlsx':
                    df_one.load_data_frame(full_file_path=t[0])
                    df_two.load_data_frame(full_file_path=t[1], sheet_name=sheet_names[os.path.basename(t[1])])
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
                    false_dict = {}
                    for i, v in enumerate(temp_list):
                        if v == True:
                            true_list.append(v)
                        if v == False:
                            # Grab the column names
                            col_t0 = str(k).split('_')[0]
                            col_t1 = str(k).split('_')[-1]
                            # Update the false_dict with the row number and values
                            #  that differed for the current column
                            # NOTE: Added an underscore to col_t1 since data will be lost if
                            #   the column names are the same as dictionary keys must be unique
                            if col_t0 not in false_dict.keys():
                                false_dict[col_t0] = [{'Row Number': i,
                                                       col_t0: df_one.data_frame[col_t0].iloc[i],
                                                       '_{}'.format(col_t1): df_two.data_frame[col_t1].iloc[i]}]
                            else:
                                false_dict[col_t0].append({'Row Number': i,
                                                       col_t0: df_one.data_frame[col_t0].iloc[i],
                                                       '_{}'.format(col_t1): df_two.data_frame[col_t1].iloc[i]})

                    # Update the true_counts dictionary
                    true_counts[k] = {'Number of Rows': len(temp_list),
                                      'Number of Identical Rows': len(true_list),
                                      'Percent of Identical Rows Between Tables(%)': float(len(true_list) / len(temp_list)),
                                      'False Values': false_dict}

                # Insert the true_counts dictionary into the master level dictionary
                self.master_dict['{}_vs_{}'.format(os.path.basename(t[0]), os.path.basename(t[1]))] = true_counts



        except Exception as e:
            print(e)







def main():
    # Files for testing
    file_one = r'C:\Users\moder\OneDrive\Desktop\sample_testing_xlsx.xlsx'
    file_two = r'C:\Users\moder\OneDrive\Desktop\sample_testing_xlsx_copy_2.xlsx'
    file_three = r'C:\Users\moder\OneDrive\Desktop\sample_testing_diff_copy.xlsx'
    file_four = r'C:\Users\moder\OneDrive\Desktop\sample_testing_xlsx_copy_3.xlsx'

    # Set up the dictionary for sheet_names
    sheet_names = {'sample_testing_xlsx.xlsx': 'Sheet1',
                  'sample_testing_xlsx_copy_2.xlsx': 'Sheet1',
                  'sample_testing_diff_copy.xlsx': 'Sheet1',
                   'sample_testing_xlsx_copy_3.xlsx': 'Sheet1'}

    # Table comparisons
    tbl_comparisons = TableComparison()
    tbl_comparisons.run_table_comparison(sheet_names, file_one, file_two, file_three, file_four)
    pprint(tbl_comparisons.master_dict)


if __name__ == '__main__':
    main()
