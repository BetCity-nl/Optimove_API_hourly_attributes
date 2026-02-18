import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.secretmanager import SecretManagerServiceClient

# ------------------------------------------------------------------------ #
# ------------------------- Google Cloud Platform ------------------------ #
# ------------------------------------------------------------------------ #


def get_optimove_api_credentials():
    """ A function to get the API credentials for Optimove from secret manager """

    secret_manager_client = SecretManagerServiceClient()
    secret_id = "projects/24200348636/secrets/optimove_api_key"
    secret_response = secret_manager_client.access_secret_version(name=f"{secret_id}/versions/latest")
    optimove_api_key = secret_response.payload.data.decode("utf-8")

    return optimove_api_key



def query_to_df(query: str):
  """ A function that creates a pandas dataframe based on a given query.
  
    Parameters
    ----------
    query : str
        The query in docstring format (BigQuery).
    project : str
        The project name inside BigQuery.
    location : str
        The location of the project/dataset in BigQuery.

    Returns
    -------
    dataframe :  pd.DataFrame
        The queried table in a pandas DataFrame format.
    """
  
  project = os.environ.get("PROJECT")

  # Run Client
  client = bigquery.Client(project=project)

  dataframe = (client.query(query)
                     .result()
                     .to_dataframe(create_bqstorage_client=True)
                  )
  
  return dataframe


def upload_table_tobq(df: pd.DataFrame, table_id: str, funcionality: str):
    """Upload a pandas dataframe to BigQuery.

    Parameters
    ----------
    df : pd.DataFrame
        The df to be uploaded to BigQuery.
    table_id : str
        The table name to be set on BigQuery, including
        the project name and dataset name. 
    funcionality: str
        The job funcionality of the function, can be either
        ['append', 'replace'].

    Returns
    -------
        A printed message for uploading confirmation.
    """

    try:

        client = bigquery.Client()

        if funcionality=="replace":

            job_config = bigquery.LoadJobConfig(skip_leading_rows=0,                    # skip the header row of the CSV file
                                                source_format=bigquery.SourceFormat.CSV,
                                                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                                                )
            
            _ = client.load_table_from_dataframe(df, table_id, job_config=job_config)

            print("Table succesfully updated in BigQuery.")
        
        elif funcionality=="append":

            job_config = bigquery.LoadJobConfig(skip_leading_rows=0,                    # skip the header row of the CSV file
                                                source_format=bigquery.SourceFormat.CSV,
                                                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                                                )
            
            _ = client.load_table_from_dataframe(df, table_id, job_config=job_config)

            print("Table succesfully updated in BigQuery.")

        else:

            print("Wrong functionality provided, there are only two options: ['append', 'replace']")

    except:
        
        print(table_id, " was not uploaded.")


def df_to_api_format(df):
    grouped = df.groupby('CustomerID')
    
    customer_list = []
    
    for customer_id, group in grouped:
        attributes = []
        for _, row in group.iterrows():
            attributes.append({
                "RealFieldName": row['RealFieldName'],
                "Value": row['Value']
            })
        
        customer_list.append({
            "CustomerID": customer_id,
            "Attributes": attributes
        })
    
    result = {
        "CustomerNewAttributesValuesList": customer_list,
        "CallbackURL": "https://europe-west1-betcity-319812.cloudfunctions.net/callback-test"
    }
    
    return result


def response_func(endpoint_url: str, headers: dict, payload: dict):
  """ A function that send the API request

  Parameters
  ----------
  endpoint_url : str
    The url of the xtremepush endpoint.
  headers : dict
    The dictionary as required by xtremepush.
  payload : dict
    The dictionary with the data (apptoken, rows, colummns).

  Returns
  -------
  response output

  """

  # Make the API request
  response = requests.post(endpoint_url,
                           headers=headers,
                           json=payload
                          )

  # Check the response status code
  if response.status_code == 200:
      print('Users data successfully pushed to OPTIMOVE API')
      print(response.content)
  elif response.status_code == 202:
      print('Users data pushed to OPTIMOVE API with code 202 and status:', response.content)
      print(response.content)
  else:
      print('Error pushing users data to OPTIMOVE API:', response.content)



users_query = f"""with

                -------------------------------------------------------------------------
                -- USERS FLAT FIELDS
                -------------------------------------------------------------------------
                usr_table as (

                select

                    username,
                    wallet_username,

                case 
                    when bonuses_not_allowed = false then "FALSE"
                    when bonuses_not_allowed = true then "TRUE"
                else "FALSE" end as bonuses_not_allowed,

                has_reached_any_limit

                from `{os.environ.get('PROJECT')}.{os.environ.get('USERS_DATASET')}.{os.environ.get('USERS_TABLE')}`

                where username like "%BETR%" and name <> "deleted:gdpr" and date(signup_date) <= current_date() -1
                and wallet_username in (SELECT distinct PlayerID from Optimove.customers_attributes)
                    
                ),

                -------------------------------------------------------------------------
                -- UNDER REVIEW FLAT FIELDS
                -------------------------------------------------------------------------
                undr_table as (

                select

                distinct

                username,
                group_type,
                email_type

                from (
                    select

                    distinct
                    username,
                    group_type,
                    email_type,
                    dwh_insert_timestamp,
                    row_number() over (partition by username order by dwh_insert_timestamp desc) as rn

                    from `{os.environ.get('PROJECT')}.{os.environ.get('OPTIMOVE_DATASET')}.under_review_optimove` 
                    )

                where username like "%BETR%" and rn = 1 
                ),

    
            -----------------------------------------------------------------------
            -- UNCLAIMED ACTIVE BONUSES PER USER --------------------------------
            bonus_counts AS (
                SELECT
                    username,
                    COUNTIF(claimed_count = 0) AS number_of_unclaimed_active_bonuses
                FROM (
                    SELECT
                        username,
                        tag_name,
                        insert_date,
                        COUNTIF(is_bonus_claimed = 1) AS claimed_count
                    FROM `betcity-319812.01_operator_data_marts.bonus_eligible_to_claim`
                    WHERE
                        SAFE_CAST(insert_date AS TIMESTAMP) < CURRENT_TIMESTAMP()
                        AND SAFE_CAST(expiration_date AS TIMESTAMP) > CURRENT_TIMESTAMP()
                    GROUP BY
                        username, tag_name, insert_date
                )
                GROUP BY
                    username
            )

            SELECT
                usrt.* EXCEPT (username),
                IFNULL(bc.number_of_unclaimed_active_bonuses, 0) AS number_of_unclaimed_active_bonuses,
                IFNULL(undr.group_type, "-") AS group_type

            FROM usr_table AS usrt
            LEFT JOIN undr_table AS undr
                ON undr.username = usrt.username
            LEFT JOIN bonus_counts AS bc
                ON bc.username = usrt.username
                            """


update_hourly_table_query = f"""
                            create or replace table `{os.environ.get('PROJECT')}.{os.environ.get('OPTIMOVE_DATASET')}.{os.environ.get('OPTI_TABLE_OPTIMOVE')}` as (

                            select

                            -- Update only values from daily change diff
                            wallet_username,
                            case when number_of_unclaimed_active_bonuses_daily <> number_of_unclaimed_active_bonuses then number_of_unclaimed_active_bonuses_daily else number_of_unclaimed_active_bonuses end as number_of_unclaimed_active_bonuses,
                            case when bonuses_not_allowed_daily <> bonuses_not_allowed then bonuses_not_allowed_daily else bonuses_not_allowed end as bonuses_not_allowed,
                            case when has_reached_any_limit_daily <> has_reached_any_limit then has_reached_any_limit_daily else has_reached_any_limit end as has_reached_any_limit,
                            case when group_type_daily <> group_type then group_type_daily else group_type end as group_type

                            from (
                                select

                                opt_hourly.*,
                                opt_daily_diff.number_of_unclaimed_active_bonuses as numer_of_unclaimed_active_bonuses_daily,
                                opt_daily_diff.has_reached_any_limit as has_reached_any_limit_daily,
                                opt_daily_diff.group_type as group_type_daily,
                                opt_daily_diff.bonuses_not_allowed as bonuses_not_allowed_daily

                                from `{os.environ.get('PROJECT')}.{os.environ.get('OPTIMOVE_DATASET')}.{os.environ.get('OPTI_TABLE_OPTIMOVE')}` as opt_hourly

                                left join (
                                            select

                                            distinct wallet_username, group_type, bonuses_not_allowed, number_of_unclaimed_active_bonuses, has_reached_any_limit

                                            from `{os.environ.get('PROJECT')}.{os.environ.get('OPTIMOVE_DATASET')}.api_attributes_daily`
                                            
                                            ) as opt_daily_diff
                                on opt_hourly.wallet_username = opt_daily_diff.wallet_username
                                )

                            )
                            