import os
import pandas as pd
from pydantic import ValidationError
from typing import List
from etl.extract import SalesRecord

def validate_and_clean_data(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """Validate and clean data by removing invalid rows and enforcing unique 'sale_id'."""
    validated_data = []
    unique_sale_ids = set()  # Track unique sale_id values

    # Loop through each DataFrame in the provided list
    for df in dataframes:
        # Replace NaN values with None for compatibility with Pydantic validation
        df = df.where(pd.notnull(df), None)

        for idx, row in df.iterrows():
            row_data = row.to_dict()
            try:
                # Validate each row using the SalesRecord model
                validated_record = SalesRecord(**row_data)
                
                # Enforce unique sale_id
                if validated_record.sale_id in unique_sale_ids:
                    print(f"Duplicate sale_id '{validated_record.sale_id}' found at row {idx}. Skipping row.")
                    continue
                
                # Add the unique sale_id to the set and the record to validated_data
                unique_sale_ids.add(validated_record.sale_id)
                validated_data.append(validated_record.dict())
                
            except ValidationError as e:
                print(f"Validation error at row {idx}: {e}")

    # Convert the list of validated records to a DataFrame
    if validated_data:
        validated_df = pd.DataFrame(validated_data)
        
        # Drop duplicate rows, just in case duplicates were added from multiple dataframes
        validated_df.drop_duplicates(subset=['sale_id'], keep=False, inplace=True)
        
        # Reset index after cleaning
        validated_df.reset_index(drop=True, inplace=True)

        print("Data validation and cleaning completed. The cleaned DataFrame shape is:", validated_df.shape)
    else:
        validated_df = pd.DataFrame()  # Return an empty DataFrame if no valid records

    return validated_df

