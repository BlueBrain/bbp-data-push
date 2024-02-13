import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from unittest.mock import patch, Mock

from bba_data_push import bba_dataset_push as test_module


DATA_DIR = Path(__file__).parent / "tests_data"


@pytest.fixture
def hierarchy_path():
    return DATA_DIR / "mba_hierarchy.json"


def test_cli_register_cell_composition_volume_distribution(tmp_path, hierarchy_path):

    # create an empty distribution to avoid registering during this test
    distribution_data = {"hasPart": []}
    input_file = tmp_path / "input_file.json"
    input_file.write_text(json.dumps(distribution_data))

    output_file = tmp_path / "output_file.json"

    mock_arglist = [
        "--input-distribution-file",
        str(input_file),
        "--output-distribution-file",
        str(output_file),
        "--reference-system-id",
        "reference-system-id",
        "--hierarchy-path",
        str(hierarchy_path),
        "--atlas-release-id",
        "atlas-release-id",
        "--atlas-release-rev",
        2,
        "--species",
        "species-id",
        "--brain-region",
        "http://api.brain-map.org/api/v2/data/Structure/997",
    ]

    config = {"forge": Mock()}

    res = CliRunner().invoke(
        test_module.cli_register_cell_composition_volume_distribution,
        mock_arglist,
        obj=config,
        catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output

    assert json.loads(output_file.read_bytes()) == distribution_data
