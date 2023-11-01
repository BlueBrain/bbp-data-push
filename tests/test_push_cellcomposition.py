import os
import logging
from datetime import datetime

from kgforge.core import Resource

from bba_data_push.bba_dataset_push import get_region_prop, create_cellComposition_prop, \
    REFSYSTEM_TYPE, VOLUME_TYPE, COMPOSITION_TYPE, COMPOSITION_ABOUT, push_cellcomposition
import bba_data_push.commons as comm


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


test_folder = os.environ["TEST_FOLDER"]
folder = os.path.join(test_folder, "tests_data")
volume_path = os.path.join(folder, "cellCompVolume.json")
summary_path = os.path.join(folder, "cellCompositionSummary_payload.json")

files_name = "GitLab unit test"
files_desc = f"{files_name} on {datetime.now()}"


def test_create_cellComposition_prop(forge, nexus_env, nexus_bucket, nexus_token,
    atlas_release_prop, brain_location_prop, subject_prop, cell_composition_id,
    contribution, base_derivation):

    cell_comp_volume = create_cellComposition_prop(forge, VOLUME_TYPE,
        COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop,
        contribution, base_derivation, files_name, files_desc, volume_path)

    assert isinstance(cell_comp_volume, Resource)
    assert VOLUME_TYPE in cell_comp_volume.type
    assert atlas_release_prop == cell_comp_volume.atlasRelease
    assert brain_location_prop == cell_comp_volume.brainLocation
    assert contribution == cell_comp_volume.contribution
    assert base_derivation in cell_comp_volume.derivation
    if volume_path:
        assert hasattr(cell_comp_volume, "distribution")
    if files_name:
        assert files_name == cell_comp_volume.name
    assert subject_prop == cell_comp_volume.subject


def test_push_cellcomposition(context, atlas_release_id, cell_composition_id,
    brain_region_id, hierarchy_path, reference_system_id, species_id):
    atlas_release_tag = "v0.5.0-rc1"
    atlas_release_rev = 29

    cell_composition = push_cellcomposition(context, atlas_release_id,
        atlas_release_rev, cell_composition_id, brain_region_id, hierarchy_path,
        reference_system_id, species_id, volume_path, summary_path, files_name,
        files_desc, atlas_release_tag, logger, False, force_registration=True, dryrun=True)

    assert isinstance(cell_composition, Resource)
    assert COMPOSITION_TYPE in cell_composition.type
    assert comm.get_property_type(atlas_release_id, comm.all_types[comm.atlasrelaseType], atlas_release_rev) == \
           cell_composition.atlasRelease
    reference_system_prop = comm.get_property_type(reference_system_id, REFSYSTEM_TYPE)
    assert reference_system_prop == cell_composition.atlasSpatialReferenceSystem
    brain_region_prop = get_region_prop(hierarchy_path, brain_region_id)
    assert comm.get_brain_location_prop(brain_region_prop, reference_system_prop) == cell_composition.brainLocation
    for cell_comp_prop in [cell_composition.cellCompositionVolume, cell_composition.cellCompositionSummary]:
        assert isinstance(cell_comp_prop, dict)
        assert "@id" in cell_comp_prop
        assert "@type" in cell_comp_prop
