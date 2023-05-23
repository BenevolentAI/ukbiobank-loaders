# ukbiobank-loaders

This repository provides an easy way to load UK Biobank data. It is composed of a pre-processing script, which converts the UK Biobank data into parquets that are easier to read,
and a library that provides different methods to access the data.

## Installation
To install this package, simply run
```bash
pip install ukbiobank-loaders
```
Please note that python 3.7 or newer is needed.

## Usage

We will now describe how to use this library. Please note that data can be read from both local directories, and aws s3 directories.

### Pre-processing
These are the UK Biobank files that are needed in order to run the pre-processing, all saved in the same directory <DATA_FOLDER>:
```
death.txt
death_cause.txt
gp_clinical.txt
gp_scripts.txt
hesin.txt
hesin_diag.txt
hesin_oper.txt
```

Additionally, also the withdrawn consent file is needed:
```
withdrawn_consent.txt
```

From the terminal, run
```bash
update_data.py --raw_dir <DATA_FOLDER> --withdrawn_file <WITHDRAWN_CONSENT_FILE_PATH> --out_dir <OUTPUT_DIR_FOLDER>
```

The processed data will be saved in a folder named `<OUTPUT_DIR_FOLDER>/final`.

We found this process to take about 14 minutes in a pod composed of 4 CPUs and 32GB of RAM. If the process is Killed, it might be
because there is not enough RAM available.

### Accessing the data

This is a simple example on how to use the library. Specific documentation about the methods is given below.
```bash
>>> from ukbb_loaders.loaders import load
>>> dl = load.DataLoader(data_dir = "<OUTPUT_DIR_FOLDER>/final")
>>> dl.get_hospital_data("icd10")
    date_of_visit source feature  value
eid
68     1986-04-22  icd10    N181      1
68     1945-05-03  icd10    N181      1
68     1950-04-03  icd10    N181      1
68     1966-08-07  icd10    N181      1
67     1991-03-12  icd10    N181      1
..            ...    ...     ...    ...
73            NaT  icd10    N181      1
48     1997-06-20  icd10    N181      1
48     1945-03-05  icd10    N181      1
48     1956-02-25  icd10    N181      1
48     1981-04-08  icd10    N181      1
```

### Documentation for ukbb\_loaders.loaders

### Table of Contents

* [ukbb\_loaders.utilities.util](#ukbb_loaders.utilities.util)
  * [load\_lookup](#ukbb_loaders.utilities.util.load_lookup)
  * [load\_mapper](#ukbb_loaders.utilities.util.load_mapper)
* [ukbb\_loaders.loaders.load](#ukbb_loaders.loaders.load)
  * [DataLoader](#ukbb_loaders.loaders.load.DataLoader)
    * [\_\_init\_\_](#ukbb_loaders.loaders.load.DataLoader.__init__)
    * [get\_hospital\_data](#ukbb_loaders.loaders.load.DataLoader.get_hospital_data)
    * [get\_death\_data](#ukbb_loaders.loaders.load.DataLoader.get_death_data)
    * [get\_gp\_clinical\_data](#ukbb_loaders.loaders.load.DataLoader.get_gp_clinical_data)
    * [get\_gp\_medication\_data](#ukbb_loaders.loaders.load.DataLoader.get_gp_medication_data)

<a id="ukbb_loaders"></a>

### ukbb\_loaders.utilities.util

<a id="ukbb_loaders.utilities.util.load_lookup"></a>

#### load\_lookup

```python
def load_lookup(lookup_name: str) -> pd.DataFrame
```

Loads lookup table.

**Arguments**:

- `lookup_name` _str_ - The name of the lookup table to be loaded.
  

**Returns**:

- `(pd.DataFrame)` - The lookup table of interest.
  

**Example**:
  Load lookup of ICD10 diagnosis codes:
  >>> load_lookup("ehr_diagnosis_icd10")
  

<a id="ukbb_loaders.utilities.util.load_mapper"></a>

#### load\_mapper

```python
def load_mapper(mapper_name: str) -> pd.DataFrame
```
Loads ontology mapper.

**Arguments**:

- `mapper_name` _str_ - The name of the mapper to be loaded.

**Returns**:

- `(pd.DataFrame)` - The mapper of interest.
  

**Example**:
  Load mapping from ICD10 codes to Phecodes:
  >>> load_mapper("icd10_to_phecodes")
  

### ukbb\_loaders.loaders.load

Loaders for versioned UKBB data.

<a id="ukbb_loaders.loaders.load.DataLoader"></a>

### DataLoader Objects

```python
class DataLoader()
```

<a id="ukbb_loaders.loaders.load.DataLoader.__init__"></a>

#### \_\_init\_\_

```python
def __init__(data_dir: str)
```

Class for loading UKBB data.

**Arguments**:

- `data_dir` _str_ - The path to the directory containing the processed data.
  Note that on Windows the path must have forward-slashes,
  e.g.  "C:/Users/john/Documents/data_dir"

<a id="ukbb_loaders.loaders.load.DataLoader.get_hospital_data"></a>

#### get\_hospital\_data

```python
def get_hospital_data(source: Union[str, List[str]],
                      level=None,
                      patient_list: np.ndarray = None) -> pd.DataFrame
```

Method that fetches hospital data for the UKBB population.

**Arguments**:

- `source` _str or list_ - The coding/representation/source we would like to fetch.
  It needs to be one or more of:
- `icd10` - for fetching all icd10 related diagnoses.
- `icd9` - for fetching all icd9 related diagnoses.
- `opcs3` - for fetching all opcs4 related operational codes.
- `opcs4` - for fetching all opcs4 related operational codes.
- `level` _list or string_ - The level/significance of diagnoses we would like to fetch.
  It needs to be one or both of:
- `primary` - for fetching only the primary code related to one diagnosis.
- `secondary` - for fetching all the secondary (complementary) codes for one
  diagnosis.
- `external` - For fetching diagnosis codes from external sources.
  Defaults to all of them.
- `patient_list` _np.ndarray_ - The patients to fetch characteristics for. If this is empty,
  all UKBB patients will be used.

**Returns**:

- `df` _pd.DataFrame_ - A long canonical dataframe with patients as the index and the
  following columns:
  - date_of_visit: pandas datetime for each hospital visit
  - feature: the different codes used (e.g. the different icd10 codes)
  - source: this is relevant to the source the feature is referring to (e.g. icd10)
  - value: the occurrence value for each row combination (initially 1.)

<a id="ukbb_loaders.loaders.load.DataLoader.get_death_data"></a>

#### get\_death\_data

```python
def get_death_data(level=None,
                   patient_list: np.ndarray = None) -> pd.DataFrame
```

Method that fetches death information for the UKBB population.

**Arguments**:

- `level` _list or string_ - The level/significance of deaths we would like to fetch.
  It needs to be one or both of: primary (main reason of death), secondary. Defaults to both.
- `patient_list` _np.ndarray_ - The patients to fetch characteristics for.
  If this is empty, all UKBB patients will be used.

**Returns**:

- `df` _pd.DataFrame_ - A long canonical dataframe with patients as the index and all
  recorded death information including death date in the right format.

<a id="ukbb_loaders.loaders.load.DataLoader.get_gp_clinical_data"></a>

#### get\_gp\_clinical\_data

```python
def get_gp_clinical_data(source=None, patient_list: np.ndarray = None)
```

Method that fetches GP diagnosis information for the UKBB population.

**Arguments**:

- `source` _str or list_ - Whether to load read_2, read_3 or both. Defaults to both.
- `patient_list` _np.ndarray_ - The patients to fetch characteristics for.
  If this is empty, all UKBB patients will be used.

**Returns**:

- `df` _pd.DataFrame_ - A long canonical dataframe with patients as the index and all
  recorded gp information including date in the right format.

<a id="ukbb_loaders.loaders.load.DataLoader.get_gp_medication_data"></a>

#### get\_gp\_medication\_data

```python
def get_gp_medication_data(patient_list: np.ndarray = None) -> pd.DataFrame
```

Method that fetches GP medication data for the UKBB population.

**Arguments**:

- `patient_list` _np.ndarray_ - The patients to fetch medication data for.
  If this is empty, all UKBB patients will be used.

**Returns**:

- `df` _pd.DataFrame_ - A canonical long dataframe with patients as the index and
  features as columns.

