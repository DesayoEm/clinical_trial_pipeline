from datetime import date
import os
import subprocess
from etl.load import Loader
from etl.transform import Transformer
from etl.utils.exceptions import NoProcessToRun
from etl.utils.log_service import progress_logger, error_logger
from config import config
from etl.extract import Extractor


class ETL:
    def __init__(self, run_extraction, run_transformation_and_load, run_dbt=False):
        self.run_extraction = run_extraction
        self.run_transformation_and_load = run_transformation_and_load
        self.run_dbt = run_dbt

        # directories for current date
        self.file_date = date.today().strftime("%Y-%m-%d")
        self.shard_dir = f"{config.SHARD_STORAGE_DIR}/{self.file_date}"
        self.compact_dir = f"{config.COMPACTED_STORAGE_DIR}/{self.file_date}"

        self.dbt_dir = config.DBT_DIR
        self.columns_to_read = config.COLUMNS_TO_READ

        self.extractor = Extractor(timeout=10, max_retries=3, pages_to_load=100)#test run
        self.transformer = Transformer(self.compact_dir)
        self.loader = Loader()

    def extract(self):
        pages_extracted = self.extractor.determine_starting_point()

        if pages_extracted >= self.extractor.pages_to_load:
            progress_logger.info(
                f"Already have {pages_extracted} pages, no further extraction needed."
            )
            return

        while pages_extracted < self.extractor.pages_to_load:
            self.extractor.make_request()
            pages_extracted += 1

        progress_logger.info(f"Extracted {pages_extracted} pages")

    def transform_and_load(self):
        progress_logger.info(f"Transforming {self.extractor.pages_to_load} pages")
        try:
            df = self.transformer.read_selective_parquet_columns(self.compact_dir, self.columns_to_read)
            progress_logger.info(f"TRANSFORMATION COMPLETE!")

            self.loader.load_to_postgres(df)
            progress_logger.info(f"LOADING COMPLETE!")

        except Exception as e:
            error_logger.error(f"Transformation failed with error: {str(e)}")
            raise



    @staticmethod
    def run_dbt_models(dbt_project_dir):
        """Execute dbt run command after data loading"""
        progress_logger.info("Starting dbt run...")

        try:
            dbt_command = ["dbt", "run"]
            dbt_command.extend(["--project-dir", dbt_project_dir])


            result = subprocess.run(
                dbt_command,
                capture_output=True,
                text=True,
                check=True
            )

            if result.stdout:
                progress_logger.info(f"dbt run output:\n{result.stdout}")

            if result.stderr:
                error_logger.error(f"dbt run stderr:\n{result.stderr}")

            if result.returncode != 0:
                error_logger.error(f"dbt run failed with exit code {result.returncode}")
                raise subprocess.CalledProcessError(
                    result.returncode,
                    dbt_command,
                    output=result.stdout,
                    stderr=result.stderr
                )
            progress_logger.info("dbt run COMPLETE YAY!")

        except subprocess.CalledProcessError as e:
            error_logger.error(f"dbt run failed with error:\n{e.stderr}")
            raise

        except FileNotFoundError:
            error_logger.error("dbt command not found.")
            raise


#For docker production, cron will run etl at 12 am, and run dbt at 1pm
#run_dbt must be False if running from docker as it has its own container, but can be true if running locally
etl = ETL(run_extraction=True, run_transformation_and_load=True, run_dbt=False)


if __name__ == "__main__":
    try:
        if (not etl.run_extraction and not
            etl.run_transformation_and_load and not etl.run_dbt
        ):
            error_logger.error("You must select a process to run")
            raise NoProcessToRun()

        if etl.run_extraction:
            etl.extract()
            os.makedirs(etl.shard_dir, exist_ok=True)
            os.makedirs(etl.compact_dir, exist_ok=True)

            with open("etl/states/last_extraction_result.py", "w") as f:
                f.write(
                    f'result = "SUCCESS"\n'
                )

            etl.extractor.compact_shards(etl.shard_dir, etl.compact_dir)

        if etl.run_transformation_and_load:
            etl.transform_and_load()


        if etl.run_dbt:
            etl.run_dbt_models(etl.dbt_dir)

        progress_logger.info(f"PIPELINE SUCCESSFUL!")

    except Exception as e:
        progress_logger.error(f"Sorry, pipeline failed: {e}")
        raise
