import sys
import io
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0,
    str(ROOT / "belgium-location"),
)

from solar.adapters.netherlands import (  # noqa: E402
    PdokBuildingTile,
)
from solar.adapters.netherlands_pipeline import (  # noqa: E402
    cache_pdok_tile,
    latest_tiles_per_sheet,
    tile_cache_filename,
    valid_zip_archive,
)


def tile(
    *,
    feature_id,
    sheet_id,
    year,
):
    return PdokBuildingTile(
        feature_id=feature_id,
        sheet_id=sheet_id,
        download_url=(
            "https://example.test/"
            f"buildings_{year}_{sheet_id}.zip"
        ),
        imagery_year=year,
        start_date=f"{year}-01-01T00:00:00Z",
        end_date=f"{year}-12-31T23:59:59Z",
    )



class FakeHttpResponse:
    def __init__(
        self,
        body,
        *,
        declared_length,
        final_url=(
            "https://3d.kadaster.nl/"
            "test/buildings.zip"
        ),
    ):
        self._stream = io.BytesIO(
            body
        )

        self.headers = {
            "Content-Length": str(
                declared_length
            )
        }

        self._final_url = final_url

    def read(
        self,
        size=-1,
    ):
        return self._stream.read(
            size
        )

    def geturl(
        self,
    ):
        return self._final_url

    def __enter__(
        self,
    ):
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        traceback,
    ):
        self._stream.close()
        return False


def cityjson_zip_bytes():
    buffer = io.BytesIO()

    with ZipFile(
        buffer,
        "w",
    ) as archive:
        archive.writestr(
            "buildings_test.city.json",
            '{"type":"CityJSON","version":"2.0"}',
        )

    return buffer.getvalue()



class NetherlandsPipelineTests(unittest.TestCase):
    def test_latest_tile_selected_independently_per_sheet(self):
        tiles = [
            tile(
                feature_id="a-old",
                sheet_id="sheet-a",
                year=2024,
            ),
            tile(
                feature_id="a-new",
                sheet_id="sheet-a",
                year=2025,
            ),
            tile(
                feature_id="b",
                sheet_id="sheet-b",
                year=2024,
            ),
        ]

        selected = latest_tiles_per_sheet(
            tiles
        )

        self.assertEqual(
            [
                (
                    item.sheet_id,
                    item.imagery_year,
                )
                for item in selected
            ],
            [
                (
                    "sheet-a",
                    2025,
                ),
                (
                    "sheet-b",
                    2024,
                ),
            ],
        )

    def test_cache_filename_comes_from_official_url(self):
        item = tile(
            feature_id="a",
            sheet_id="136000_454000",
            year=2025,
        )

        self.assertEqual(
            tile_cache_filename(
                item
            ),
            (
                "buildings_2025_"
                "136000_454000.zip"
            ),
        )

    def test_valid_zip_requires_cityjson_member(self):
        with tempfile.TemporaryDirectory() as temp:
            path = (
                Path(temp)
                / "valid.zip"
            )

            with ZipFile(
                path,
                "w",
            ) as archive:
                archive.writestr(
                    "example.city.json",
                    "{}",
                )

            self.assertTrue(
                valid_zip_archive(
                    path
                )
            )

    def test_zip_without_cityjson_is_not_valid_pdok_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            path = (
                Path(temp)
                / "invalid.zip"
            )

            with ZipFile(
                path,
                "w",
            ) as archive:
                archive.writestr(
                    "readme.txt",
                    "test",
                )

            self.assertFalse(
                valid_zip_archive(
                    path
                )
            )


    def test_cache_retries_after_incomplete_download(self):
        item = tile(
            feature_id="retry",
            sheet_id="90000_436000",
            year=2025,
        )

        complete = cityjson_zip_bytes()

        truncated = complete[
            : max(
                1,
                len(complete) // 2,
            )
        ]

        with tempfile.TemporaryDirectory() as temp:
            cache_dir = Path(
                temp
            )

            responses = [
                FakeHttpResponse(
                    truncated,
                    declared_length=len(
                        complete
                    ),
                ),
                FakeHttpResponse(
                    complete,
                    declared_length=len(
                        complete
                    ),
                ),
            ]

            with patch(
                "solar.adapters.netherlands_pipeline.urlopen",
                side_effect=responses,
            ) as mocked:
                result = cache_pdok_tile(
                    item,
                    cache_dir=cache_dir,
                    attempts=2,
                    retry_delay_s=0.0,
                )

            self.assertEqual(
                mocked.call_count,
                2,
            )

            self.assertTrue(
                result.exists()
            )

            self.assertTrue(
                valid_zip_archive(
                    result
                )
            )

            self.assertEqual(
                result.read_bytes(),
                complete,
            )


if __name__ == "__main__":
    unittest.main()
