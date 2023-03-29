import argparse
import logging


def get_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--std_dir",
        type=str,
        required=True,
        help="The directory of standardised and typed files.",
    )
    parser.add_argument(
        "--final_dir",
        type=str,
        required=True,
        help="The directory of final derived files.",
    )
    return parser.parse_args()


def init_logger(module_name: str):
    logging.basicConfig(
        format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    return logger
