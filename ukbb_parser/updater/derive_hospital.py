"""
Processing script to derive the final hospital files.
"""
import traceback
from os.path import join as pjoin

import pandas as pd

from ukbb_parser.updater.utils import get_args, init_logger

logger = init_logger(__name__)


def main(std_dir: str, final_dir: str):
    """
    Load and format the hospital data.
    """
    # Load admission information
    logger.info("Loading hospital admission data.")
    df = pd.read_parquet(pjoin(std_dir, "hesin.parquet"))
    df["date"] = df["epistart"].fillna(df["admidate"])
    df = df[["eid", "ins_index", "date"]]

    # Load diagnosis information
    logger.info("Loading hospital diagnosis data.")
    df_diag = pd.read_parquet(pjoin(std_dir, "hesin_diag.parquet"))
    df_diag = df_diag.rename({"level": "source"}, axis=1)

    # Format and save ICD9 and ICD10 data
    for icd in [9, 10]:
        logger.info(f"Formatting and saving ICD{icd} data.")
        df_icd = (
            df_diag[["eid", "ins_index", "source", f"diag_icd{icd}"]].dropna().copy()
        )
        df_icd = df_icd.merge(
            df, left_on=["eid", "ins_index"], right_on=["eid", "ins_index"], how="left"
        )
        df_icd = df_icd.rename({f"diag_icd{icd}": "feature"}, axis=1)
        df_icd = df_icd[["eid", "date", "source", "feature"]].set_index("eid")
        df_icd.to_parquet(
            pjoin(final_dir, f"ehr_diagnosis_icd{icd}.parquet"),
        )
        del df_icd
    del df_diag

    # Load operation information
    logger.info("Loading hospital operation data.")
    df_oper = pd.read_parquet(pjoin(std_dir, "hesin_oper.parquet"))
    df_oper = df_oper.rename({"level": "source"}, axis=1)
    df_oper["opdate"] = pd.to_datetime(df_oper["opdate"])

    # Format and save opcs3 and opcs4
    for opcs in [3, 4]:
        logger.info(f"Formatting and saving OPER{opcs} data.")
        df_opcs = (
            df_oper[["eid", "ins_index", "source", f"oper{opcs}", "opdate"]]
            .dropna()
            .copy()
        )
        df_opcs = df_opcs.merge(
            df, left_on=["eid", "ins_index"], right_on=["eid", "ins_index"], how="left"
        )
        df_opcs["opdate"] = df_opcs["opdate"].fillna(df_opcs["date"])
        df_opcs = df_opcs.drop(columns=["ins_index", "date"])
        df_opcs = df_opcs.rename({"opdate": "date", f"oper{opcs}": "feature"}, axis=1)
        df_opcs = df_opcs[["eid", "date", "source", "feature"]].set_index("eid")
        df_opcs.to_parquet(
            pjoin(final_dir, f"ehr_procedures_opcs{opcs}.parquet"),
        )
        del df_opcs
    del df_oper, df


if __name__ == "__main__":
    args = get_args()

    # Run main bit of the function
    try:
        logger.info("Creating final HES files.")
        main(std_dir=args.std_dir, final_dir=args.final_dir)
        logger.info("All HES files have been created successfully.")

    # Write trace of error
    except Exception:
        logger.error(traceback.format_exc())
        raise
