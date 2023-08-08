import pytest
import  great_expectations as gx
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from src.jobs import dim_customer
class TestDataValidation:
    @pytest.fixture(autouse=True)
    def set_up(self):
        df=dim_customer.dim_customer_df
        return  df
    def test_column_count(self,set_up):
        assert len(set_up.columns) == 29,"column count is not as expected "
