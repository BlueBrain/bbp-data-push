import pytest
from pathlib import Path
from click.testing import CliRunner

from bba_dataset_push.bba_data_push import initialize_pusher_cli

TEST_PATH = Path(Path(__file__).parent.parent)


def test_initialize_pusher_cli():

    # initialize-pusher-cli argument data
    forge_config_file = str(Path(TEST_PATH, "tests/test_forge_config/test_forge_config_demo.yaml"))
    nexus_org = "some_org"
    nexus_proj = "some_proj"
    nexus_token_file = str(Path(TEST_PATH, "tests/test_forge_config/empty_token.txt"))

    # push-meshes argument data
    dataset_path = str(Path(TEST_PATH, "tests/tests_data/test_brain_region_meshes_hybrid"))
    config = str(Path(TEST_PATH, "tests/tests_data/test_push_dataset_config_template.yaml"))
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/test_hierarchy.json"))

    #
    wrong_forge_config_file = str(
        Path(TEST_PATH, "tests/test_forge_config/wrong_config/test_forge_config_demo.yaml")
    )
    wrong_token_file = str(Path(TEST_PATH, "tests/test_forge_config/wrong_config/wrong_token/"))

    # environment exception
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            initialize_pusher_cli,
            [
                "--forge_config_file",
                forge_config_file,
                "--nexus_env",
                "wrong_env",
                "--nexus_org",
                nexus_org,
                "--nexus_proj",
                nexus_proj,
                "--nexus_token_file",
                nexus_token_file,
                "push-meshes",
                "--dataset_path",
                dataset_path,
                "--config",
                config,
                "--hierarchy_path",
                hierarchy_path,
            ],
        )
        assert result.exit_code == 1

    # wrong forge config file exception
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            initialize_pusher_cli,
            [
                "--forge_config_file",
                wrong_forge_config_file,
                "--nexus_env",
                "staging",
                "--nexus_org",
                nexus_org,
                "--nexus_proj",
                nexus_proj,
                "--nexus_token_file",
                nexus_token_file,
                "push-meshes",
                "--dataset_path",
                dataset_path,
                "--config",
                config,
                "--hierarchy_path",
                hierarchy_path,
            ],
        )
        assert result.exit_code == 1

    # wrong token file
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            initialize_pusher_cli,
            [
                "--forge_config_file",
                forge_config_file,
                "--nexus_env",
                "staging",
                "--nexus_org",
                nexus_org,
                "--nexus_proj",
                nexus_proj,
                "--nexus_token_file",
                wrong_token_file,
                "push-meshes",
                "--dataset_path",
                dataset_path,
                "--config",
                config,
                "--hierarchy_path",
                hierarchy_path,
            ],
        )
        assert result.exit_code == 1
