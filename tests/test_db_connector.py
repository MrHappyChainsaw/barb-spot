import unittest
from unittest.mock import patch
import barb_spot as bs
import pandas as pd

class TestFetchData(unittest.TestCase):
    
    @patch('barb_spot.db_connector.get_db_connection')
    @patch('pandas.read_sql')
    def test_fetch_data(self, mock_read_sql, mock_get_db_connection):
        # Mock the return value of the SQL query
        mock_read_sql.return_value = pd.DataFrame({
            'SalesAreaNo': [1, 2],
            'DemoNumber': [123, 456],
            'YearMonth': ['202401', '202401'],
            'DurationImpacts': [100, 200],
            'RatecardImpacts': [150, 300]
        })

        # Test the fetch_data function
        result = bs.fetch_data('2024-01-01', '2024-01-31')
        
        # Assert the DataFrame is as expected
        self.assertEqual(result.shape, (2, 5))
        self.assertIn('SalesAreaNo', result.columns)

if __name__ == '__main__':
    unittest.main()