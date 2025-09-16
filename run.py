"""
CLI entry point for the HolesValidator project.
Reads a Parquet file, computes unreachable hole warnings/errors,
and saves the processed DataFrame to a new Parquet file.
"""

import argparse
import logging
import sys

from src.holesvalidator import HolesValidator


def main():
    parser = argparse.ArgumentParser(
        description="Process a Parquet file containing manufacturing data")
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input Parquet local file"
    )
    parser.add_argument(
        "output_file",
        type=str,
        help="Path to output Parquet file where results will be saved"
    )
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stdout
    )
    logger = logging.getLogger("HolesValidator")

    logger.info(f"Processing file: {args.input_file}")
    validator = HolesValidator(parquet_path=args.input_file)

    # Process the file
    df_processed = validator.process()
    logger.info(
        f"Processing completed. Number of rows processed: {len(df_processed)}")

    # Save output
    df_processed.to_parquet(args.output_file, index=False)
    logger.info(f"Processed file saved to: {args.output_file}")


if __name__ == "__main__":
    main()
