import unittest
import pandas as pd
import os
import csv
import logging
import sys
sys.path.insert(1, "../../ingestion")

from combine_data import combine_data
class CombineTest(unittest.TestCase):
    def test_combine(self):
        """
        Test the combine data function for combining our stripped data
        """
        # log that we are beginning our test
        log = logging.getLogger("test")
        log.debug("Testing Combine Data")
        log.debug(os.getcwd())
        # call combine_data on the sample stripped_data
        combine_data()
        # save our combined_data file from our combine_data function into the dataframe combined_data_test
        combined_data_test = pd.read_csv("./combined_data/combinedData.csv")
        # save expected combined_data file from our combine_data_true folder into the dataframe combined_data_expected
        combined_data_expected = pd.read_csv("./combined_data_true/combinedData.csv")
        # compare our combined_data_test dataframe with our expected combined_data dataframe
        pd.testing.assert_frame_equal(combined_data_test.reset_index(drop=True),combined_data_expected.reset_index(drop=True))
        # remove combinedData that was created
        os.remove("./combined_data/combinedData.csv")
        pass

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("test").setLevel(logging.DEBUG)
    # start test
    print(os.getcwd())
    unittest.main()
