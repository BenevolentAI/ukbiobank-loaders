"""
Script to load the UKBB data to typed parquet files.
"""
import argparse
import logging
import traceback
from typing import List, Tuple

from os.path import join as pjoin

import numpy as np
import pandas as pd

logging.basicConfig(
    format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw_dir",
        type=str,
        required=True,
        help="Directory where the raw files are stored.",
    )
    parser.add_argument(
        "--withdrawn_file",
        type=str,
        required=True,
        help="File with withdrawn consent eids",
    )
    parser.add_argument(
        "--std_dir",
        type=str,
        required=True,
        help="Output directory where standardised files will be saved.",
    )
    return parser.parse_args()


def _postprocess_df(
    df: pd.DataFrame, categories: list, withdrawn_eids: list
) -> pd.DataFrame:
    """
    Type categorical fields and type.
    """
    for col in categories:
        df[col] = df[col].astype("category")
    df = df.loc[~df.eid.isin(withdrawn_eids)]
    return df


def get_hesin_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "ins_index": np.int16,
        "epistart": "str",
        "admidate": "str",
    }
    dates = ["epistart", "admidate"]
    categories: List[str] = []
    return dtypes, dates, categories


def get_hesin_diag_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "ins_index": np.int16,
        "level": np.int8,
        "diag_icd9": "str",
        "diag_icd10": "str",
    }
    categories = ["diag_icd9", "diag_icd10"]
    dates: List[str] = []
    return dtypes, dates, categories


def get_hesin_oper_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "ins_index": np.int16,
        "level": np.int8,
        "oper3": "str",
        "oper4": "str",
        "opdate": "str",
    }
    categories = ["oper3", "oper4"]
    dates = ["opdate"]
    return dtypes, dates, categories


def get_death_cause_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "ins_index": np.int8,
        "arr_index": np.int8,
        "level": np.int8,
        "cause_icd10": "str",
    }
    categories = ["cause_icd10"]
    dates: List[str] = []
    return dtypes, dates, categories


def get_death_dtypes() -> Tuple[dict, list, list]:
    dtypes = {"eid": np.int32, "date_of_death": "str"}
    categories: List[str] = []
    dates = ["date_of_death"]
    return dtypes, dates, categories


def get_gp_clinical_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "data_provider": np.int8,
        "event_dt": "str",
        "read_2": "str",
        "read_3": "str",
        "value1": "object",
        "value2": "object",
        "value3": "object",
    }
    categories = ["read_2", "read_3"]
    dates = ["event_dt"]
    return dtypes, dates, categories


def get_gp_scripts_dtypes() -> Tuple[dict, list, list]:
    dtypes = {
        "eid": np.int32,
        "data_provider": np.int8,
        "issue_date": "str",
        "drug_name": "str",
        "quantity": "str",
    }
    categories: List[str] = []
    dates = ["issue_date"]
    return dtypes, dates, categories


def main(
    raw_dir: str,
    std_dir: str,
    withdrawn_file: str,
):
    # Get patients who withdrew consent
    withdrawn = pd.read_csv(withdrawn_file, header=None)

    # Process hospital files
    logger.info("Loading hesin")
    dtypes, dates, categories = get_hesin_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "hesin.txt"),
        dtype=dtypes,
        parse_dates=dates,
        dayfirst=True,
        infer_datetime_format=True,
        usecols=dtypes.keys(),
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "hesin.parquet"))
    logger.info("Saved processed hesin")
    del df

    logger.info("Loading hesin_diag")
    dtypes, _, categories = get_hesin_diag_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "hesin_diag.txt"), dtype=dtypes, usecols=dtypes.keys()
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "hesin_diag.parquet"))
    logger.info("Saved processed hesin_diag")
    del df

    logger.info("Loading hesin_oper")
    dtypes, dates, categories = get_hesin_oper_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "hesin_oper.txt"),
        dtype=dtypes,
        parse_dates=dates,
        dayfirst=True,
        infer_datetime_format=True,
        usecols=dtypes.keys(),
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "hesin_oper.parquet"))
    logger.info("Saved processed hesin_oper")
    del df

    # Process death data
    logger.info("Loading death_cause")
    dtypes, _, categories = get_death_cause_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "death_cause.txt"), dtype=dtypes, usecols=dtypes.keys()
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "death_cause.parquet"))
    logger.info("Saved processed death_cause")
    del df

    logger.info("Loading death")
    dtypes, dates, categories = get_death_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "death.txt"),
        dtype=dtypes,
        parse_dates=dates,
        dayfirst=True,
        infer_datetime_format=True,
        usecols=dtypes.keys(),
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "death.parquet"))
    logger.info("Saved processed death")
    del df

    # Process clinical data
    logger.info("Loading gp_clinical")
    dtypes, dates, categories = get_gp_clinical_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "gp_clinical.txt"),
        dtype=dtypes,
        encoding="latin1",
        parse_dates=dates,
        dayfirst=True,
        infer_datetime_format=True,
        usecols=dtypes.keys(),
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "gp_clinical.parquet"))
    logger.info("Saved processed gp_clinical")
    del df

    logger.info("Loading gp_scripts")
    dtypes, dates, categories = get_gp_scripts_dtypes()
    df = pd.read_table(
        pjoin(raw_dir, "gp_scripts.txt"),
        dtype=dtypes,
        encoding="latin1",
        parse_dates=dates,
        dayfirst=True,
        infer_datetime_format=True,
        usecols=dtypes.keys(),
    )
    df = _postprocess_df(df=df, categories=categories, withdrawn_eids=list(withdrawn[0]))
    df.to_parquet(pjoin(std_dir, "gp_scripts.parquet"))
    logger.info("Saved processed gp_scripts")
    del df


if __name__ == "__main__":
    args = get_args()

    # Run main logic of the code
    try:
        logger.info("Running standardisation and typing on the raw files.")
        main(
            raw_dir=args.raw_dir,
            std_dir=args.std_dir,
            withdrawn_file=args.withdrawn_file,
        )
        logger.info("Standardisation and typing completed successfully.")

    # Write trace of error
    except Exception:
        logger.error(traceback.format_exc())
        raise
