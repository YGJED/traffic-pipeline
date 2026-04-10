# traffic-pipeline


#### Architecutre Overview


##### Preprocessing Instructions
Put the NRIX dataset from Box into `/preprocessing-scripts` and run `prune.py` followed by `consolidate_parquet.py` followed by `upload_parquets.py`
Create a `.env` file in the project (e.g. repo root) with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN / AWS_REGION. load_dotenv() loads it when you run the script so boto3 sees those vars—no credentials in source code.

TODO: 
 - Put more information about setting up the S3 bucket and make sure that the code in there works with easily changed variables
 - Could also create a jupyter notebook or .sh that just runs those 3 files one after the other
 - Put more information about what each `.py` file does



