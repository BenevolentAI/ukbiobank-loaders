"""
Processing script to derive the final death registry files.
"""
import argparse
import logging
import traceback
from os.path import join as pjoin

import pandas as pd

from ukbb_parser.updater.utils import get_args, init_logger

logger = init_logger(__name__)


def main(std_dir: str, final_dir: str):
    """
    Load and format the death data.
    """
    logger.info("Loading death causes.")
    df = pd.read_parquet(pjoin(std_dir, "death_cause.parquet"))
    df = df.rename({"cause_icd10": "feature"}, axis=1).drop(
        columns=["ins_index", "arr_index"]
    )
    df = df.set_index("eid")

    # Load death dates
    logger.info("Loading death dates.")
    df_dates = pd.read_parquet(pjoin(std_dir, "death.parquet"))
    df_dates = (
        df_dates[["eid", "date_of_death"]]
        .drop_duplicates()
        .set_index("eid")["date_of_death"]
    )
    df["date"] = df.index.map(df_dates)
    df = df.reset_index().drop_duplicates().set_index("eid")
    del df_dates

    # Splitting into primary and secondary to match cohort.yaml
    logger.info("Saving death_icd10_primary.")
    df_primary = df.loc[df["level"] == 1, ["date", "feature"]]
    df_primary.to_parquet(
        pjoin(final_dir, "death_icd10_primary.parquet"),
    )

    logger.info("Saving death_icd10_secondary.")
    df_secondary = df.loc[df["level"] == 2, ["date", "feature"]]
    df_secondary.to_parquet(
        pjoin(final_dir, "death_icd10_secondary.parquet"),
    )

    del df, df_primary, df_secondary


if __name__ == "__main__":
    args = get_args()

    # Run main bit of the function
    try:
        logger.info("Creating final death registry files.")
        main(std_dir=args.std_dir, final_dir=args.final_dir)
        logger.info("All death registry files have been created successfully.")

    # Write trace of error
    except Exception:
        logger.error(traceback.format_exc())
        raise
