from datetime import date
import os
from src.etl_scripts.load import Loader
from src.etl_scripts.transform import Transformer
from src.utils.log_service import progress_logger, error_logger
from src.config import config
from src.etl_scripts.extract import Extractor


class ETL:
    def __init__(self, run_extraction, run_transformation, run_load):
        self.run_extraction = run_extraction
        self.run_transformation = run_transformation
        self.run_load = run_load


        self.file_date = date.today().strftime("%Y-%m-%d")

        #directoties for current date
        self.shard_dir = f"{config.SHARD_STORAGE_DIR}/{self.file_date}"
        self.compact_dir = f"{config.COMPACTED_STORAGE_DIR}/{self.file_date}"

        self.columns_to_read = config.COLUMNS_TO_READ
        self.extractor = Extractor(timeout=10, max_retries=3,pages_to_load=10)
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


    def transform(self):
        progress_logger.info(f"Transforming {self.extractor.pages_to_load} pages")
        try:
            self.transformer.read_selective_parquet_columns(self.compact_dir, self.columns_to_read)
            progress_logger.info(f"TRANSFORMATION COMPLETE YAY!")

        except Exception as e:
            error_logger.error(f"Transformation failed with error: {str(e)}")
            raise

    def load(self):
        pass



etl = ETL(True, True, False)
if __name__ == "__main__":

    try:
        if etl.run_extraction:
            etl.extract()
            os.makedirs(etl.shard_dir, exist_ok=True)
            os.makedirs(etl.compact_dir, exist_ok=True)
            etl.extractor.compact_shards(etl.shard_dir, etl.compact_dir)

        if etl.run_transformation:
            etl.transform()

        # if etl.run_load:
        #     etl.load()

        progress_logger.info(f"SUCCESSFUL! HIGH FIVE!")

    except Exception as e:
        progress_logger.error(f"Sorry o, pipeline failed: {e}")
        raise


