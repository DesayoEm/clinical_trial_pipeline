from datetime import date
import os
from etl.load import Loader
from etl.transform import Transformer
from etl.utils.log_service import progress_logger, error_logger
from config import config
from etl.extract import Extractor


class ETL:
    def __init__(self, run_extraction, run_transformation_and_load):
        self.run_extraction = run_extraction
        self.run_transformation_and_load = run_transformation_and_load


        self.file_date = date.today().strftime("%Y-%m-%d")

        #directoties for current date
        self.shard_dir = f"{config.SHARD_STORAGE_DIR}/{self.file_date}"
        self.compact_dir = f"{config.COMPACTED_STORAGE_DIR}/{self.file_date}"

        self.columns_to_read = config.COLUMNS_TO_READ
        self.extractor = Extractor(timeout=10, max_retries=3,pages_to_load=2)
        self.transformer = Transformer(self.compact_dir)
        self.loader = Loader()


    def extract(self):
        pages_extracted = self.extractor.determine_starting_point()

        if pages_extracted >= self.extractor.pages_to_load:
            progress_logger.info(
                f"Already have {pages_extracted} pages, no extraction needed."
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
            progress_logger.info(f"TRANSFORMATION COMPLETE YAY!")

            self.loader.load_to_postgres(df)
            progress_logger.info(f"LOADING COMPLETE YAY!")

        except Exception as e:
            error_logger.error(f"Transformation failed with error: {str(e)}")
            raise




etl = ETL(True, False)
if __name__ == "__main__":

    try:
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

        progress_logger.info(f"PIPELINE SUCCESSFUL! HIGH FIVE!")

    except Exception as e:
        progress_logger.error(f"Sorry o, pipeline failed: {e}")
        raise


