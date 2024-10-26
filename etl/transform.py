import os
import pandas as pd
from pydantic import ValidationError
from typing import List
from etl.extract import SalesRecord

def validate_and_clean_data(dataframes: List[pd.DataFrame]):
    """Validate and clean data by removing invalid rows and duplicates."""
    validated_data = []
    
    for df in dataframes:
        # Replace NaN values with None for Pydantic compatibility
        df = df.where(pd.notnull(df), None)

        for idx, row in df.iterrows():
            row_data = row.to_dict()
            try:
                validated_record = SalesRecord(**row_data)
                validated_data.append(validated_record.dict())
            except ValidationError as e:
                print(f"Validation error at row {idx}: {e}")

    # Create a DataFrame from validated data
    validated_df = pd.DataFrame(validated_data)
    
    # Drop duplicate rows
    validated_df.drop_duplicates(inplace=True)
    
    # Reset index after cleaning
    validated_df.reset_index(drop=True, inplace=True)

    print("Data validation and cleaning completed. The combined DataFrame shape is:", validated_df.shape)
    return validated_df
