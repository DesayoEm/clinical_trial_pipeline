from sqlalchemy import create_engine
from src.config import config
from typing import Dict
import pandas as pd
from src.utils.log_service import progress_logger, error_logger

class Loader:
    def __init__(self):
        self.conn_str = config.DATABASE_CONNECTION_STRING

    def load_to_postgres(self, dataframes: pd.DataFrame):
        engine = create_engine(self.conn_str)

        load_order = [
            'studies',
            'sponsors',
            'conditions',
            'interventions',
            'sites',
            'study_sponsors',
            'study_conditions',
            'study_interventions',
            'study_sites'
        ]

        try:
            with engine.begin() as conn:
                for table_name in load_order:
                    if table_name in dataframes and not dataframes[table_name].empty:
                        df = dataframes[table_name]
                        progress_logger.info(f"Loading {table_name}: {len(df)} rows")

                        df.to_sql(
                            name=table_name,
                            con=conn,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )

                        progress_logger.info(f" {table_name} loaded: {len(df)} rows")

            progress_logger.info("All tables loaded successfully!")

        except Exception as e:
            error_logger.error(f"Don't cry my love, Load failed, rolling back: {str(e)}")
            raise

        finally:
            engine.dispose()






