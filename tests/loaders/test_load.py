"""
Testing datasets/ukbb/loaders/load.py
"""
import pytest
import pandas as pd
import numpy as np

from unittest.mock import patch, Mock

from ukbb_loaders.loaders import load

DATA_DIR = "s3://data_path"
@pytest.fixture()
def mock_s3_contents():
    return [
        "data_path/random.parquet"
        "data_path/versioned_data/random2.parquet"
    ]


@pytest.fixture()
def raw_ehr_diagnosis_icd10():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['N181', 'N182', 'N181'],
            "date": ['2010-02-16', '2015-03-31', '1913-09-04'],
            "source": [1, 2, 2],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_ehr_diagnosis_icd9():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['585', '585', '585'],
            "date": ['1986-02-13', '1995-11-01', '1992-11-12'],
            "source": [1, 2, 2],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_ehr_procedures_opcs3():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['4695', '4695', '4695'],
            "date": ['2002-04-23', '2007-10-28', '2012-03-06'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_ehr_procedures_opcs4():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['X403', 'X403', 'X403'],
            "date": ['2003-04-23', '2008-10-28', '2013-03-06'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_procedures():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "date": ['2014-06-17', '2015-08-10', '2016-06-01'],
            "feature": ['abc', 'abc', 'abc'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def dummy_opcs_codes():
    return {'code1': 'description1', 'code2': 'description2'}


@pytest.fixture()
def raw_death_icd10_primary():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['N181', 'N182', 'N181'],
            "date": ['2016-07-10', '2015-04-25', '2014-01-14'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_death_icd10_secondary():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['N181', 'N182', 'N181'],
            "date": ['2016-07-10', '2015-04-25', '2014-01-14'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_ehr_diagnosis_read2():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['79010', '79010', '79010'],
            "date": ['2012-04-24', '2014-02-06', '2011-10-06'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_ehr_diagnosis_read3():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "feature": ['XaA1S', 'XaA1S', 'XaA1S'],
            "date": ['2010-10-29', '2014-02-10', '2008-04-14'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df


@pytest.fixture()
def raw_trait_measurements():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "date": ['2014-06-17', '2015-08-10', '2016-06-01'],
            "feature": ['abc', 'abc', 'abc'],
            "value": [11.1, 22.2, 33.3],
            'unit': ['xyz', 'xyz', 'xyz'],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df



@pytest.fixture()
def raw_medications():
    df = pd.DataFrame(
        {
            "eid": [1, 2, 3],
            "date": ['2014-06-17', '2015-08-10', '2016-06-01'],
            "feature": ['abc', 'abc', 'abc'],
            "value": [1, 1, 1],
            "source": [1, 1, 1],
        }
    ).set_index("eid")
    df['date'] = pd.to_datetime(df['date'])
    return df



@patch("ukbb_loaders.loaders.load.S3FileSystem")
def test_init_data_path(mock_s3fs: Mock, mock_s3_contents: list):
    mock_s3fs().ls.return_value = mock_s3_contents
    actual = load.DataLoader(DATA_DIR).data_path
    expect = "s3://data_path"
    assert actual == expect


def test_error_local_directory():
    with pytest.raises(FileNotFoundError):
        load.DataLoader("fake_dir")


@patch("ukbb_loaders.loaders.load.S3FileSystem")
def test_init_hospital_map(mock_s3fs: Mock, mock_s3_contents: list):
    mock_s3fs().ls.return_value = mock_s3_contents
    actual = load.DataLoader("s3://data_path").hospital_map
    expect = {
        "icd9": "ehr_diagnosis_icd9.parquet",
        "icd10": "ehr_diagnosis_icd10.parquet",
        "opcs3": "ehr_procedures_opcs3.parquet",
        "opcs4": "ehr_procedures_opcs4.parquet",
    }
    assert actual == expect


@patch("ukbb_loaders.loaders.load.S3FileSystem")
def test_init_gp_map(mock_s3fs: Mock, mock_s3_contents: list):
    mock_s3fs().ls.return_value = mock_s3_contents
    actual = load.DataLoader(DATA_DIR).gp_map
    expect = {"read_2": "ehr_diagnosis_read2.parquet", "read_3": "ehr_diagnosis_read3.parquet"}
    assert actual == expect


@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_death_data(
    mock_read_parquet: Mock, mock_s3fs: Mock, raw_death_icd10_primary, mock_s3_contents: list
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.return_value = raw_death_icd10_primary
    actual = load.DataLoader(DATA_DIR).get_death_data(level='primary', patient_list=np.array([1, 2]))
    expect = pd.DataFrame(
        {
            "eid": [1, 2],
            "feature": ['N181', 'N182'],
            "date_of_death": ['2016-07-10', '2015-04-25'],
            "source": ['primary', 'primary'],
            "value": [1, 1],
        }
    ).set_index(['eid'])
    expect['date_of_death'] = pd.to_datetime(expect['date_of_death'])

    pd.testing.assert_frame_equal(actual, expect)


@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_death_data_all(
    mock_read_parquet: Mock,
    mock_s3fs: Mock,
    raw_death_icd10_primary,
    raw_death_icd10_secondary,
    mock_s3_contents: list,
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.side_effect = [raw_death_icd10_primary, raw_death_icd10_secondary]
    actual = load.DataLoader(DATA_DIR).get_death_data()
    expect = pd.DataFrame(
        {
            "eid": [1, 2, 3, 1, 2, 3],
            "feature": ['N181', 'N182', 'N181', 'N181', 'N182', 'N181'],
            "date_of_death": [
                '2016-07-10',
                '2015-04-25',
                '2014-01-14',
                '2016-07-10',
                '2015-04-25',
                '2014-01-14',
            ],
            "source": ['primary', 'primary', 'primary', 'secondary', 'secondary', 'secondary'],
            "value": [1, 1, 1, 1, 1, 1],
        }
    ).set_index(['eid'])
    expect['date_of_death'] = pd.to_datetime(expect['date_of_death'])

    pd.testing.assert_frame_equal(actual, expect)


@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_hospital_data(
    mock_read_parquet: Mock,
    mock_s3fs: Mock,
    raw_ehr_diagnosis_icd9,
    mock_s3_contents: list,
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.return_value = raw_ehr_diagnosis_icd9
    actual = load.DataLoader(DATA_DIR).get_hospital_data(
        source="icd9", level="primary", patient_list=np.array([1, 2]),
    )
    expect = pd.DataFrame(
        {
            "eid": [1],
            "feature": ['585'],
            "date_of_visit": ['1986-02-13'],
            "source": ['icd9'],
            "value": [1],
        }
    ).set_index(['eid'])
    expect['date_of_visit'] = pd.to_datetime(expect['date_of_visit'])

    pd.testing.assert_frame_equal(actual, expect)


@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_hospital_data_all(
    mock_read_parquet: Mock,
    mock_s3fs: Mock,
    raw_ehr_diagnosis_icd10,
    raw_ehr_diagnosis_icd9,
    mock_s3_contents: list,
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.side_effect = [raw_ehr_diagnosis_icd10, raw_ehr_diagnosis_icd9]
    actual = load.DataLoader(DATA_DIR).get_hospital_data(
        source=["icd10", "icd9"], level=["primary", "secondary"]
    )
    expect = pd.DataFrame(
        {
            "eid": [1, 2, 3, 1, 2, 3],
            "feature": ['N181', 'N182', 'N181', '585', '585', '585'],
            "date_of_visit": [
                '2010-02-16',
                '2015-03-31',
                '1913-09-04',
                '1986-02-13',
                '1995-11-01',
                '1992-11-12',
            ],
            "source": ['icd10', 'icd10', 'icd10', 'icd9', 'icd9', 'icd9'],
            "value": [1, 1, 1, 1, 1, 1],
        }
    ).set_index(['eid'])
    expect['date_of_visit'] = pd.to_datetime(expect['date_of_visit'])

    pd.testing.assert_frame_equal(actual, expect)



@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_gp_clinical_data(
    mock_read_parquet: Mock, mock_s3fs: Mock, raw_ehr_diagnosis_read2, mock_s3_contents: list
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.return_value = raw_ehr_diagnosis_read2
    actual = load.DataLoader(DATA_DIR).get_gp_clinical_data(source='read_2', patient_list=np.array([1, 2]))
    expect = pd.DataFrame(
        {
            "eid": [1, 2],
            "feature": ['79010', '79010'],
            "date_of_visit": ['2012-04-24', '2014-02-06'],
            "source": ['read_2', 'read_2'],
            "value": [1, 1],
        }
    ).set_index(['eid'])
    expect['date_of_visit'] = pd.to_datetime(expect['date_of_visit'])

    pd.testing.assert_frame_equal(actual, expect)


@patch("ukbb_loaders.loaders.load.S3FileSystem")
@patch("ukbb_loaders.loaders.load.pd.read_parquet")
def test_get_gp_clinical_data_all(
    mock_read_parquet: Mock,
    mock_s3fs: Mock,
    raw_ehr_diagnosis_read2,
    raw_ehr_diagnosis_read3,
    mock_s3_contents: list,
):
    mock_s3fs().ls.return_value = mock_s3_contents
    mock_read_parquet.side_effect = [raw_ehr_diagnosis_read2, raw_ehr_diagnosis_read3]
    actual = load.DataLoader(DATA_DIR).get_gp_clinical_data()
    expect = pd.DataFrame(
        {
            "eid": [1, 2, 3, 1, 2, 3],
            "feature": ['79010', '79010', '79010', 'XaA1S', 'XaA1S', 'XaA1S'],
            "date_of_visit": [
                '2012-04-24',
                '2014-02-06',
                '2011-10-06',
                '2010-10-29',
                '2014-02-10',
                '2008-04-14',
            ],
            "source": ['read_2', 'read_2', 'read_2', 'read_3', 'read_3', 'read_3'],
            "value": [1, 1, 1, 1, 1, 1],
        }
    ).set_index(['eid'])
    expect['date_of_visit'] = pd.to_datetime(expect['date_of_visit'])

    pd.testing.assert_frame_equal(actual, expect)


def test_check_arg():
    with pytest.raises(ValueError) as context:
        load._check_arg(given='x', accepted=['a', 'b', 'c'], arg_type='test')
    assert str(context.value) == "The test argument should be one of ['a', 'b', 'c']"


