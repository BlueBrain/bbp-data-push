import os
import logging
from datetime import datetime

from bba_data_push.bba_dataset_push import create_cellComposition_prop, VOLUME_TYPE, COMPOSITION_ABOUT


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


test_folder = os.environ["TEST_FOLDER"]
folder = os.path.join(test_folder, "tests_data")


def test_push_cellcomposition(forge, nexus_env, nexus_bucket, nexus_token,
    atlas_release_prop, brain_location_prop, subject_prop, cell_composition_id,
    contribution, base_derivation):
    if os.environ.get("FULL_TEST"):  # to be set manually when running the pipeline interactively
        volume_file = "cellCompVolume.json"
    else:
        volume_file = "cellCompVolume_small.json"
    volume_path = os.path.join(folder, volume_file)

    files_name = "GitLab unit test"
    files_desc = f"{files_name} on {datetime.now()}"

    cell_comp_volume = create_cellComposition_prop(forge, VOLUME_TYPE, COMPOSITION_ABOUT,
        atlas_release_prop, brain_location_prop, subject_prop, contribution,
        base_derivation, files_name, files_desc, volume_path)

    assert cell_comp_volume is not None
