from datetime import datetime, date
import requests
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict

from etl.utils.exceptions import NextPageError, FailedRequestError, MissingStateError, FileCompactionError
from etl.utils.rate_limit import RateLimiterHandler
from etl.utils.log_service import progress_logger, error_logger
from config import config
from etl.states import last_token, last_shard_path, last_extraction_result




class Extractor:
    def __init__(self, timeout, max_retries,pages_to_load):
        self.current_page = self.determine_starting_point()
        #technically current page should be  last saved + 1 but the val is incremented
        #by one in the make_requests func so no need to do it here

        self.last_saved_page = self.determine_starting_point()

        self.url = config.BASE_URL
        self.token = last_token.last_saved_token

        self.next_page_url = f"{config.PAGES_BASE_URL}{self.token}"
        self.timeout = timeout
        self.max_retries = max_retries

        self.pages_to_load = pages_to_load
        self.rate_limit_handler = RateLimiterHandler()

        progress_logger.info(
            f"Initializing Extractor \n \n"
            f"\n Last saved page is page {self.last_saved_page}"
            f"\n URL is {self.url}"
            f"\n Next URL is {self.next_page_url}"
            )


    @staticmethod
    def determine_starting_point():
        state_file = f"{config.STATE_MGT_DIR}/last_extraction_result.py"

        if not os.path.exists(state_file):
            raise MissingStateError("extraction_result")

        if last_extraction_result.result == "SUCCESS":
            return 0 #start fresh extraction as last one was successful

        output_dir = f"{last_shard_path.shard_path}"

        files = os.listdir(output_dir)
        parquet_files = [f for f in files if f.endswith(".parquet")]

        return len(parquet_files) if parquet_files else 0


    def make_request(self):
        url = self.url if not self.current_page else self.next_page_url

        self.current_page += 1
        progress_logger.info(f"Starting from page {self.current_page}")

        with open("etl/states/last_extraction_result.py", "w") as f:
            f.write(
                f'result = "IN PROGRESS"\n'
            )

        self.rate_limit_handler.wait_if_needed()

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    data = response.json()
                    next_page_token = data.get("nextPageToken")

                    if not next_page_token:
                        raise NextPageError(self.current_page)

                    with open("etl/states/last_token.py", "w") as f:
                        f.write(
                            f'last_saved_token = "{next_page_token}"\n'
                        )

                    self.next_page_url = f"{config.PAGES_BASE_URL}{next_page_token}"

                    progress_logger.info(
                        f'Successfully made request to {url} \n Last loaded page is page {self.current_page}'
                        f'\n Next page token is {next_page_token}'
                        f'\n Next page is {self.next_page_url}'
                    )

                    return self.save_response(data)

            except Exception as e:
                with open("etl/states/last_extraction_result.py", "w") as f:
                    f.write(
                         f'result = "FAILURE"\n'
                    )

                error_logger.warning(
                    f"Request exception on attempt {attempt + 1}/{self.max_retries}: {e}"
                )

                raise FailedRequestError(self.current_page, str(e))

        error_logger.warning(
                f"Request exception FAILED AFTER 3 attempts on page {self.current_page}"
            )


    def save_response(self, data: Dict):
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        file_date = datetime.today().strftime("%Y-%m-%d")

        output_dir = f"{config.SHARD_STORAGE_DIR}/{file_date}"
        os.makedirs(output_dir, exist_ok=True)

        with open("etl/states/last_shard_path.py", "w") as f:
            f.write(
                f'shard_path = "{output_dir}"\n'
            )
        page_number = self.current_page
        file_to_write = f"{output_dir}/{page_number}.parquet"

        pq.write_table(table, file_to_write)

        self.last_saved_page += 1


        progress_logger.info(
            f"Successfully saved page {self.current_page} at {file_to_write}"
        )



    @staticmethod
    def compact_shards(path_to_read: str, path_to_write: str):
        try:
            files = os.listdir(path_to_read)

            file_name = f"studies - {date.today().strftime("%Y-%m-%d")}"
            file_to_write = f"{path_to_write}/{file_name}.parquet"

            parquet_shards = [f for f in files if f.endswith(".parquet")]
            num_of_files = len(parquet_shards)

            if num_of_files == 0:
                progress_logger.info("No parquet files to compact.")
                return

            writer = None
            try:
                for file in parquet_shards:
                    table = pq.read_table(f"{path_to_read}/{file}")

                    if writer is None:
                        writer = pq.ParquetWriter(file_to_write, table.schema)

                    if table.schema != writer.schema:
                        table = table.cast(writer.schema)

                    writer.write_table(table)

            except Exception as e:
                if writer and writer.is_open:
                    writer.close()
                progress_logger.error(f"Compaction failed. Shards preserved at: {path_to_read}\n Error: {str(e)}")

            finally:
                if writer and writer.is_open:
                    writer.close()

            progress_logger.info(
                f"{num_of_files} pages compacted at {file_to_write}"
            )
        except Exception as e:
            raise FileCompactionError(str(e))



