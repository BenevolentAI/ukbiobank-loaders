"""
Loaders for versioned UKBB data.
"""
import logging
import os
from os.path import join as pjoin
from typing import List, Union

import numpy as np
import pandas as pd
from s3fs import S3FileSystem

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataLoader:
    def __init__(
            self,
            data_dir: str,
    ):
        """
        Class for loading UKBB data.
        Args:
            data_dir (str): The path to the directory containing the processed data. Note that on Windows the path must
            have forward-slashes, e.g.  "C:/Users/john/Documents/data_dir"
        """
        self.data_path = self._check_if_exists(data_dir=data_dir)
        self.hospital_map = {
            "icd9": "ehr_diagnosis_icd9.parquet",
            "icd10": "ehr_diagnosis_icd10.parquet",
            "opcs3": "ehr_procedures_opcs3.parquet",
            "opcs4": "ehr_procedures_opcs4.parquet",
        }
        self.gp_map = {
            "read_2": "ehr_diagnosis_read2.parquet",
            "read_3": "ehr_diagnosis_read3.parquet",
        }

    def _check_if_exists(self, data_dir: str) -> str:
        """
        Checks if the requested directory exists and returns it.
        """
        if data_dir.startswith("s3://"):
            s3_file = S3FileSystem()
            files_in_version = s3_file.ls(data_dir)
        else:
            files_in_version = os.listdir(data_dir)

        if not files_in_version:
            raise ValueError(
                f"""{data_dir} seems to be empty. Check if the files are there of if you have permission access to 
                {data_dir}."""
            )

        return data_dir

    def get_hospital_data(
            self,
            source: Union[str, List[str]],
            level=None,
            patient_list: np.ndarray = None,
    ) -> pd.DataFrame:
        """
        Args:
            source (str or list): The coding/representation/source we would like to fetch.
                It needs to be one or more of:
                    icd10: for fetching all icd10 related diagnoses.
                    icd9: for fetching all icd9 related diagnoses.
                    opcs3: for fetching all opcs4 related operational codes.
                    opcs4: for fetching all opcs4 related operational codes.
            level (list or string): The level/significance of diagnoses we would like to fetch.
                It needs to be one or both of:
                    primary: for fetching only the primary code related to one diagnosis.
                    secondary: for fetching all the secondary (complementary) codes for one
                        diagnosis.
                    external: For fetching diagnosis codes from external sources.
                Defaults to all of them.
            patient_list (np.ndarray): The patients to fetch characteristics for. If this is empty,
                all UKBB patients will be used.
        Returns:
            df (pandas dataframe): A long canonical dataframe with patients as the index and the
            following columns:
                - date_of_visit: pandas datetime for each hospital visit
                - feature: the different codes used (e.g. the different icd10 codes)
                - source: this is relevant to the source the feature is referring to (e.g. icd10)
                - value: the occurrence value for each row combination (initially 1.)
        """
        # Check if source and level got one of the accepted values
        if level is None:
            level = ["primary", "secondary", "external"]
        _check_arg(given=source, accepted=self.hospital_map, arg_type="source")
        _check_arg(given=level, accepted=["primary", "secondary", "external"], arg_type="level")
        sources = _to_list_type(source)
        levels = _to_list_type(level)
        levels = [{"primary": 1, "secondary": 2, "external": 3}[lev] for lev in levels]

        # Reading data
        df_list: List[pd.DataFrame] = []
        for src in sources:
            df = pd.read_parquet(pjoin(self.data_path, self.hospital_map[src]))
            if (patient_list is not None) and (len(patient_list) > 0):
                df = df.loc[df.index.isin(patient_list)]
            df = df.loc[df["source"].isin(levels)]
            df["source"] = src
            df_list.append(df)
        df = pd.concat(df_list).rename({"date": "date_of_visit"}, axis=1)
        df["value"] = 1

        return df

    def get_death_data(
            self,
            level=None,
            patient_list: np.ndarray = None,
    ) -> pd.DataFrame:
        """
        Method that fetches death information for the UKBB population.

        Args:
            level (list or string): The level/significance of deaths we would like to fetch.
                It needs to be one or both of: primary (main reason of death), secondary. Defaults to both.
            patient_list (np.ndarray): The patients to fetch characteristics for.
                If this is empty, all UKBB patients will be used.
        Returns:
            df (pandas dataframe): A long canonical dataframe with patients as the index and all
                recorded death information including death date in the right format.
        """
        if level is None:
            level = ["primary", "secondary"]
        _check_arg(given=level, accepted=["primary", "secondary"], arg_type="level")
        levels = _to_list_type(level)

        df_list: List[pd.DataFrame] = []
        for level in levels:
            df = pd.read_parquet(pjoin(self.data_path, f"death_icd10_{level}.parquet"))
            if (patient_list is not None) and (len(patient_list) > 0):
                df = df.loc[df.index.isin(patient_list)]
            df["source"] = level
            df_list.append(df)
        df = pd.concat(df_list)
        df["value"] = 1

        return df.rename({"date": "date_of_death"}, axis=1)

    def get_gp_clinical_data(
            self, source=None,
            patient_list: np.ndarray = None
    ):
        """
        Method that fetches gp diagnosis information for the UKBB population.
        Args:
            source (str or list): Whether to load read_2, read_3 or both. Defaults to both.
            patient_list (np.ndarray): The patients to fetch characteristics for.
                If this is empty, all UKBB patients will be used.
        Returns:
            df (pandas dataframe): A long canonical dataframe with patients as the index and all
                recorded gp information including date in the right format.
        """
        # Check if source got one of the accepted values
        if source is None:
            source = ["read_2", "read_3"]
        _check_arg(given=source, accepted=self.gp_map, arg_type="source")
        sources = _to_list_type(source)

        df_list: List[pd.DataFrame] = []
        for src in sources:
            df = pd.read_parquet(pjoin(self.data_path, self.gp_map[src]))
            if (patient_list is not None) and (len(patient_list) > 0):
                df = df.loc[df.index.isin(patient_list)]
            df["source"] = src
            df_list.append(df)
        df = pd.concat(df_list)
        df["value"] = 1

        return df.rename({"date": "date_of_visit"}, axis=1)

    def get_gp_medication_data(self, patient_list: np.ndarray = None) -> pd.DataFrame:
        """
        Args:
            patient_list (np.ndarray): The patients to fetch medication data for.
                If this is empty, all UKBB patients will be used.
        Returns:
            df (pandas dataframe): A canonical long dataframe with patients as the index and
                features as columns.
        """
        df = pd.read_parquet(pjoin(self.data_path, "gp_medications.parquet"))
        if (patient_list is not None) and (len(patient_list) > 0):
            df = df.loc[df.index.isin(patient_list)]
        df = df.rename({"date": "date_of_issue"}, axis=1)
        return df


def _to_list_type(value: Union[int, str, list, np.ndarray]) -> Union[list, np.ndarray]:
    """
    If a value is not a list or an array, then convert to a list.
    """
    return value if isinstance(value, (list, np.ndarray)) else [value]


def _check_arg(given: Union[int, str, list], accepted: Union[list, dict], arg_type: str = "source"):
    # Check if given value to arg is one of the accepted ones
    givens = given if isinstance(given, (list, np.ndarray)) else [given]
    for given in givens:
        if given not in accepted:
            raise ValueError(f"The {arg_type} argument should be one of {accepted}")
