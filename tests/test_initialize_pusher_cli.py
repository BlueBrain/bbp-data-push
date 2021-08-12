from pathlib import Path
from click.testing import CliRunner

from bba_dataset_push.bba_data_push import initialize_pusher_cli

TEST_PATH = Path(Path(__file__).parent.parent)


def test_initialize_pusher_cli():

    # initialize-pusher-cli argument data
    forge_config_file = str(
        Path(TEST_PATH, "tests/test_forge_config/test_forge_config_demo.yaml")
    )
    nexus_org = "some_org"
    nexus_proj = "some_proj"
    nexus_token = "nexus token"

    # push-meshes argument data
    dataset_path = str(
        Path(TEST_PATH, "tests/tests_data/test_brain_region_meshes_hybrid")
    )
    config = str(
        Path(TEST_PATH, "tests/tests_data/test_push_dataset_config_template.yaml")
    )
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/test_hierarchy.json"))

    #
    wrong_forge_config_file = str(
        Path(
            TEST_PATH,
            "tests/test_forge_config/wrong_config/test_forge_config_demo.yaml",
        )
    )
    wrong_token = "wrong token"

    # environment exception
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            initialize_pusher_cli,
            [
                "--forge-config-file",
                forge_config_file,
                "--nexus-env",
                "wrong-env",
                "--nexus-org",
                nexus_org,
                "--nexus-proj",
                nexus_proj,
                "--nexus-token",
                nexus_token,
                "push-meshes",
                "--dataset-path",
                dataset_path,
                "--config",
                config,
                "--hierarchy-path",
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
                "--forge-config-file",
                wrong_forge_config_file,
                "--nexus-env",
                "staging",
                "--nexus-org",
                nexus_org,
                "--nexus-proj",
                nexus_proj,
                "--nexus-token",
                nexus_token,
                "push-meshes",
                "--dataset-path",
                dataset_path,
                "--config",
                config,
                "--hierarchy-path",
                hierarchy_path,
            ],
        )
        assert result.exit_code == 1

    # wrong token
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            initialize_pusher_cli,
            [
                "--forge-config-file",
                forge_config_file,
                "--nexus-env",
                "staging",
                "--nexus-org",
                nexus_org,
                "--nexus-proj",
                nexus_proj,
                "--nexus-token",
                wrong_token,
                "push-meshes",
                "--dataset-path",
                dataset_path,
                "--config",
                config,
                "--hierarchy-path",
                hierarchy_path,
            ],
        )
        assert result.exit_code == 1
