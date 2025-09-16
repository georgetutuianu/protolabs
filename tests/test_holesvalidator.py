import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.holesvalidator import HolesValidator


class TestHolesValidator:

    # --------------------------
    # Tests for _parse_holes
    # --------------------------
    def test_parse_holes_valid_list(self):
        validator = HolesValidator("dummy_path")
        json_str = '[{"length": 5, "radius": 2}, {"length": 15, "radius": 3}]'
        result = validator._parse_holes(json_str, uuid="test1")
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(h, dict) for h in result)

    def test_parse_holes_single_dict(self):
        validator = HolesValidator("dummy_path")
        json_str = '[{"length": 5, "radius": 2}]'
        result = validator._parse_holes(json_str, uuid="test2")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["length"] == 5

    def test_parse_holes_nan(self):
        validator = HolesValidator("dummy_path")
        result = validator._parse_holes(float("nan"), uuid="test3")
        assert result == []

    def test_parse_holes_invalid_json(self, caplog):
        validator = HolesValidator("dummy_path")
        invalid_json = '{"length": 5, "radius": 2'  # missing closing brace
        with caplog.at_level("WARNING"):
            result = validator._parse_holes(invalid_json, uuid="test4")
        assert result == []
        assert any("Could not parse holes JSON" in rec.message for rec in caplog.records)

    # --------------------------
    # Tests for _process_chunk
    # --------------------------
    def test_process_chunk_flags(self):
        validator = HolesValidator("dummy_path")
        data = {
            "uuid": ["a", "b", "c"],
            "holes": [
                '[{"length": 81, "radius": 1}]',  # triggers both warn and error
                '[{"length": 21, "radius": 1}]',  # triggers warn only
                '[]'  # triggers neither
            ]
        }
        df_chunk = pd.DataFrame(data)
        processed = validator._process_chunk(df_chunk)

        assert bool(processed.loc[0, "has_unreachable_hole_warning"]) is True
        assert bool(processed.loc[0, "has_unreacheable_hole_error"]) is True

        assert bool(processed.loc[1, "has_unreachable_hole_warning"]) is True
        assert bool(processed.loc[1, "has_unreacheable_hole_error"]) is False

        assert bool(processed.loc[2, "has_unreachable_hole_warning"]) is False
        assert bool(processed.loc[2, "has_unreacheable_hole_error"]) is False

    # --------------------------
    # Integration test for process()
    # --------------------------
    def test_process_integration(self, tmp_path):
        validator = HolesValidator(str(tmp_path / "test.parquet"))

        # Create small Parquet file
        data = {
            "uuid": ["x", "y"],
            "holes": ['[{"length": 248, "radius": 2.15}]', '[{"length": 25, "radius": 1}]']
        }
        table = pa.Table.from_pandas(pd.DataFrame(data))
        pq.write_table(table, tmp_path / "test.parquet")

        # Process file
        df_processed = validator.process()

        assert df_processed.shape[0] == 2
        assert bool(df_processed.loc[0, "has_unreachable_hole_warning"]) is True
        assert bool(df_processed.loc[0, "has_unreacheable_hole_error"]) is True
        assert bool(df_processed.loc[1, "has_unreachable_hole_warning"]) is True
        assert bool(df_processed.loc[1, "has_unreacheable_hole_error"]) is False
