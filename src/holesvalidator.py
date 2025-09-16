import json
import logging
import pyarrow.parquet as pq
import pandas as pd

class HolesValidator:
    warn_factor = 2 * 10
    error_factor = 2 * 40

    # will only read the columns needed
    columns = ["uuid", "holes"]

    def __init__(self, parquet_path: str):
        """
        Args:
            parquet_path: Path to the Parquet file.
        """
        self.parquet_path = parquet_path

    def _parse_holes(self, holes_str, uuid):
        """
        Safely parse JSON string of holes into a list of dicts.
        """
        if pd.isna(holes_str):
            return []

        try:
            parsed = json.loads(holes_str)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            logging.warning(f"UUID {uuid}: Could not parse holes JSON - {holes_str}")
            return []  # return valid list if anything fails

    def _process_chunk(self, df_chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single DataFrame chunk: parse JSON, compute flags.
        """
        # Parse holes JSON safely
        df_chunk["holes_json"] = [
            self._parse_holes(holes, uuid=row_uuid)
            for holes, row_uuid in zip(df_chunk["holes"], df_chunk.get("uuid"))
        ]

        # Compute warning and error flags in a single pass
        warnings, errors = [], []
        for holes in df_chunk["holes_json"]:
            has_warn, has_err = False, False

            for hole in holes:
                length, radius = hole.get("length", 0), hole.get("radius", 0)
                if not has_warn and length > radius * self.warn_factor:
                    has_warn = True
                if not has_err and length > radius * self.error_factor:
                    has_err = True

                if has_warn and has_err:
                    break  # stop to avoid making unnecessary checks
            warnings.append(has_warn)
            errors.append(has_err)

        df_chunk["has_unreachable_hole_warning"] = warnings
        df_chunk["has_unreacheable_hole_error"] = errors

        # Drop temporary JSON column
        df_chunk.drop("holes_json", axis=1, inplace=True)

        return df_chunk

    def process(self) -> pd.DataFrame:
        """
        Process the Parquet file in chunks and return a DataFrame with flags.
        """
        parquet_file = pq.ParquetFile(self.parquet_path)  # read only the metadata
        processed_chunks = []

        # now read the table in chunks, one by one
        for row_group in range(parquet_file.num_row_groups):
            chunk_table = parquet_file.read_row_group(row_group, columns=self.columns)
            # transform to pandas df to use it easier
            df_chunk = chunk_table.to_pandas()

            # process the current chunk
            df_chunk = self._process_chunk(df_chunk)

            # maybe even write chunks to disk so we don't keep them in  memory
            processed_chunks.append(df_chunk)

        # glue everything back together
        df_processed = pd.concat(processed_chunks, ignore_index=True)
        return df_processed
