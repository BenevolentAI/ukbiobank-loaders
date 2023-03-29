#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess
import traceback
from os.path import join as pjoin
import ukbb_parser.updater.standardise_raw as standardise_raw
import ukbb_parser.updater.derive_gp as derive_gp
import ukbb_parser.updater.derive_hospital as derive_hospital
import ukbb_parser.updater.derive_death as derive_death

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
        "--out_dir",
        type=str,
        required=True,
        help="Output directory where standardised files will be saved.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    std_dir = pjoin(args.out_dir, "standardised")
    final_dir = pjoin(args.out_dir, "final")
    if not args.out_dir.startswith("s3://"):
        os.makedirs(std_dir, exist_ok=True)
        os.makedirs(final_dir, exist_ok=True)

    script_dir = os.path.dirname(os.path.realpath(__file__))
    try:
        standardise_raw.main(raw_dir=args.raw_dir, std_dir=std_dir, withdrawn_file=args.withdrawn_file)

        derive_gp.main(std_dir=std_dir, final_dir=final_dir)
        derive_hospital.main(std_dir=std_dir, final_dir=final_dir)
        derive_death.main(std_dir=std_dir, final_dir=final_dir)
    # Write trace of error
    except Exception:
        logger.error(traceback.format_exc())
        raise
