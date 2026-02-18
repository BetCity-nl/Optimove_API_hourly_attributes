import os
import pandas as pd
import numpy as np
import time
from datetime import datetime
import pytz

from helpers import query_to_df, upload_table_tobq, users_query, update_hourly_table_query, df_to_api_format, response_func, get_optimove_api_credentials

def main():

    api_url_add = os.environ.get("API_URL_ADD_ATTRIBUTES")
    API_KEY = get_optimove_api_credentials()
    PROJECT = os.environ.get("PROJECT")
    OPTIMOVE_DATASET = os.environ.get("OPTIMOVE_DATASET")
    OPTI_TABLE_OPTIMOVE = os.environ.get("OPTI_TABLE_OPTIMOVE")
    OPTI_TABLE_DIFFERENCE = os.environ.get("OPTI_TABLE_DIFFERENCE")

    # Check if it's 6 AM in Amsterdam (that's when the daily optimove update has run)
    # Change by George 8/1/24 time to check from 6 to 5
    # Commented out by George 10/1/24 as part of trying to fix discrepancies between BQ and Optimove
    if datetime.now(pytz.timezone('Europe/Amsterdam')).hour == 4:

        _ = query_to_df(update_hourly_table_query)

        print("Wait 5 mins for the hourly table to be updated with the latest updates from the daily Update and the batch upload to finish.")
        time.sleep(5*60)


    df_optimove = query_to_df(query=f"select * from `{PROJECT}.{OPTIMOVE_DATASET}.{OPTI_TABLE_OPTIMOVE}`")

    df_database = query_to_df(query=users_query)

    print(f"The optimove dataset in BQ has: {df_optimove.shape[0]} users & then database dataset has: {df_database.shape[0]}")

    # Columns associated with the HOURLY Update
    hourly_differences_df = (pd.concat([
                                    df_database.assign(status="database"),
                                    df_optimove.assign(status="optimove")
                                    ])
                            .drop_duplicates(keep=False)
                            .loc[lambda x: x["status"] == "database"]
                            .reset_index(drop=True)
                        )

    print(f"The total hourly differences (between Optimove and Database) are: {hourly_differences_df.shape[0]}")
    print("Upload hourly differences in BQ...")


    # UPLOAD hourly differences to be pushed in OPTIMOVE
    upload_table_tobq(df = hourly_differences_df, 
                      table_id = f'{PROJECT}.{OPTIMOVE_DATASET}.{OPTI_TABLE_DIFFERENCE}', 
                      funcionality = "replace")


    # Replace OPTIMOVE dataset in BQ with new push (Assuming all new updated (diffs) were pushed correctly)
    df_to_upload_bq = pd.concat([
                                df_optimove.loc[lambda x: ~x["wallet_username"].isin(list(hourly_differences_df["wallet_username"]))],
                                hourly_differences_df
                                ],
                                axis=0).reset_index(drop=True)

    time.sleep(2) # wait 2 seconds 
    print("Upload new Optimove dataset in BQ...")

    local_test = 'betcity-319812.test_jaime.api_attributes_hourly_current_optimove_table_test_jaime'

    upload_table_tobq(df = df_to_upload_bq, 
                      table_id = f'{PROJECT}.{OPTIMOVE_DATASET}.{OPTI_TABLE_OPTIMOVE}', 
                      funcionality = "replace")

    # Rename columns to match expected input from Optimove
    old_cols = list(hourly_differences_df.keys()) # To be used in the logs
    hourly_differences_df = hourly_differences_df.rename(columns={"wallet_username": "CustomerID", 
                                                                  "bonuses_not_allowed": "DO_NOT_ALLOW_BONUS",
                                                                  "has_reached_any_limit": "HAS_REACHED_ANY_LIMIT",
                                                                  "number_of_unclaimed_active_bonuses": "NUMBER_OF_UNCLAIMED_ACTIVE_BONUSES",
                                                                  "group_type": "UR_PLAYERS"})
    try:
        hourly_differences_log_df = pd.DataFrame(data=hourly_differences_df.values, columns=old_cols)
        hourly_differences_log_df["update_time"] = pd.to_datetime(datetime.now()) #assume it is UTC+1
        
        upload_table_tobq(df = hourly_differences_log_df,
                            table_id = f"{PROJECT}.{OPTIMOVE_DATASET}.hourly_differences_logs",
                            funcionality = 'append')
    except Exception as e:
        print("Error updating logs table: ",e)

    headers = {'content-type': 'application/json', 'accept': 'application/json',  "X-API-KEY": f"{API_KEY}"}

    print("PUSH hourly differences in OPTIMOVE API...")

    if len(hourly_differences_df["CustomerID"].unique().tolist()) > 1000:

        print(f"The number of rows is more than the API server can handle. Create batches of this dataset of 1000 users per call.")

        batches = np.array_split(hourly_differences_df["CustomerID"].to_numpy(), len(hourly_differences_df) // 1000 + 1)

        for num, batch in enumerate(batches):
        
            # Melting the DataFrame to match OPTIMOVE API expected format
            df_melted = pd.melt(hourly_differences_df.loc[lambda x: x["CustomerID"].isin(batch)].reset_index(drop=True),
                                id_vars=['CustomerID'],
                                var_name='RealFieldName', 
                                value_name='Value')
            
            json_output = df_to_api_format(df_melted)

            response_func(endpoint_url=api_url_add, headers=headers, payload=json_output)

            time.sleep(2) # wait 2 seconds before next post request.
    
    else:

        # Melting the DataFrame to match OPTIMOVE API expected format
        df_melted = pd.melt(hourly_differences_df,
                            id_vars=['CustomerID'],
                            var_name='RealFieldName', 
                            value_name='Value')
        
        json_output = df_to_api_format(df_melted)

        response_func(endpoint_url=api_url_add, headers=headers, payload=json_output)

    return "DONE!!!"

if __name__ == "__main__":

    main()
