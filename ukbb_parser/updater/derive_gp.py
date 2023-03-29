"""
Processing script to derive the final gp files.
"""
import traceback
from datetime import datetime
from os.path import join as pjoin

import pandas as pd

from ukbb_parser.updater.utils import get_args, init_logger

logger = init_logger(__name__)


def main(std_dir: str, final_dir: str):
    logger.info("Deriving read2/read3 diagnoses files.")
    df = pd.read_parquet(pjoin(std_dir, "gp_clinical.parquet"))
    df = df.rename({"event_dt": "date"}, axis=1)
    for read_version in [2, 3]:
        logger.info(f"Formatting read_{read_version}.")
        df_new = df[["eid", "date", f"read_{read_version}"]].dropna().copy()
        df_new = df_new.rename({f"read_{read_version}": "feature"}, axis=1)
        df_new = df_new.drop_duplicates().set_index("eid")

        # Save data
        logger.info(f"Saving read_{read_version}.")
        df_new.to_parquet(
            pjoin(final_dir, f"ehr_diagnosis_read{read_version}.parquet"),
        )
        del df_new

    logger.info("Deriving GP medication data")
    df = pd.read_parquet(pjoin(std_dir, "gp_scripts.parquet"))
    df = df.loc[df["issue_date"] <= datetime.now()]
    df = df.rename({"issue_date": "date", "drug_name": "feature"}, axis=1)
    df = df[["eid", "date", "feature"]].dropna().set_index("eid")

    logger.info("Saving GP medication data")
    df.to_parquet(pjoin(final_dir, "gp_medications.parquet"))


if __name__ == "__main__":
    args = get_args()

    # Run main bit of the function
    try:
        logger.info("Creating final GP files.")
        main(std_dir=args.std_dir, final_dir=args.final_dir)
        logger.info("All GP files have been created successfully.")

    # Write trace of error
    except Exception:
        logger.error(traceback.format_exc())
        raise
