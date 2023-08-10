import os
import logging
from datetime import datetime

from bba_data_push.bba_dataset_push import create_cellComposition_prop, VOLUME_TYPE, COMPOSITION_ABOUT, get_subject_prop, get_derivation
from bba_data_push.commons import return_contribution

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


test_folder = os.environ["TEST_FOLDER"]
folder = os.path.join(test_folder, "tests_data")


def test_push_cellcomposition(forge, nexus_env, nexus_bucket, nexus_token, atlas_release_id, cell_composition_id, brain_region_id,
                              reference_system_id, species_id):
    if os.environ.get("FULL_TEST"):  # to be set manually when running the pipeline interactively
        volume_file = "cellCompVolume.json"
    else:
        volume_file = "cellCompVolume_small.json"
    volume_path = os.path.join(folder, volume_file)
    summary_path = os.path.join(folder, "density_stats_small.json")

    class Ctx:
        obj = {
            "forge": forge,
            "env": nexus_env,
            "bucket": nexus_bucket,
            "token": nexus_token}

    files_name = "GitLab unit test"
    files_desc = f"{files_name} on {datetime.now()}"
    tag = f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}"

    atlas_release_prop = {"@id": atlas_release_id,
                          "@type": ["AtlasRelease", "BrainAtlasRelease"]}
    species_prop = {"@id": species_id, "label": "Mus Musculus"}
    subject_prop = get_subject_prop(species_prop)
    brain_region_prop = {"@id": brain_region_id, "label": "root"}
    reference_system_prop = {"@id": reference_system_id,
                             "@type": ['AtlasSpatialReferenceSystem',
                                       'BrainAtlasSpatialReferenceSystem']}
    brain_location = {
        "brainRegion": brain_region_prop,
        "atlasSpatialReferenceSystem": reference_system_prop}

    contribution, _ = return_contribution(forge, nexus_bucket, nexus_token,
                                          organization="staging" not in nexus_env)
    derivation = get_derivation(atlas_release_id)

    cell_comp_volume = create_cellComposition_prop(Ctx.obj["forge"], VOLUME_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location, subject_prop, contribution,
        derivation, files_name, files_desc, volume_path)

    assert cell_comp_volume is not None
