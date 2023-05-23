from pathlib import Path

import pandas as pd

DIRECTORY_ROOT = Path(__file__).parent.parent.absolute()
LOOKUP_PATH = DIRECTORY_ROOT / "files" / "lookups"
MAPPERS_PATH = DIRECTORY_ROOT / "files" / "mappers"

def load_lookup(lookup_name: str) -> pd.DataFrame:
    """
    Loads lookup table.

    Args:
        lookup_name (str): The name of the lookup table to be loaded.

    Returns:
        (pd.DataFrame): The lookup table of interest.

    Example:
        >>> load_lookup("ehr_diagnosis_icd10")
        Returns the lookup table containing ICD10 diagnosis information.
    """

    return pd.read_parquet(LOOKUP_PATH / f"{lookup_name}.parquet")

def load_mapper(mapper_name: str) -> pd.DataFrame:
    """
    Loads ontology mapper.

    Args:
        mapper_name (str): The name of the mapper to be loaded.

    Returns:
        (pd.DataFrame): The mapper of interest.

    Example:
        >>> load_mapper("icd10_to_phecodes")
        Returns the mapping from ICD10 codes to Phecodes.
    """

    return pd.read_parquet(MAPPERS_PATH / f"{mapper_name}.parquet")
