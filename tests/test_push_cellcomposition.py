import os
import json
import logging
from datetime import datetime

from kgforge.core import Resource

from bba_data_push.bba_dataset_push import get_region_prop, create_cellComposition_prop, \
    REFSYSTEM_TYPE, VOLUME_TYPE, COMPOSITION_TYPE, COMPOSITION_ABOUT, push_cellcomposition, get_subject_prop
from bba_data_push.push_cellComposition import register_densities, part_key, id_key
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


def test_push_cellcomposition(forge, atlas_release_id, cell_composition_id,
    brain_region_id, hierarchy_path, reference_system_id, species_id):
    atlas_release_tag = "v0.5.0-rc1"
    atlas_release_rev = 29

    cell_composition = push_cellcomposition(forge, atlas_release_id,
        atlas_release_rev, cell_composition_id, brain_region_id, hierarchy_path,
        reference_system_id, species_id, volume_path, summary_path, files_name,
        files_desc, atlas_release_tag, logger, force_registration=True, dryrun=True)

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


def test_register_densities(forge, atlas_release_prop, brain_region_id, hierarchy_path,
    reference_system_prop, species_id, cell_composition_id, contribution, base_derivation):

    brain_region_prop = get_region_prop(hierarchy_path, brain_region_id)
    brain_location_prop = comm.get_brain_location_prop(brain_region_prop, reference_system_prop)
    species_prop = comm.get_property_label(comm.Args.species, species_id, forge)
    subject_prop = get_subject_prop(species_prop)
    resource_tag = "GitLab-test"

    volume_path_all_ids = os.path.join(folder, "cellCompVolume_small.json")
    volume_path_id_only = volume_path_all_ids.replace(".json", "_id_only.json")
    volume_id_only_content = register_densities(volume_path_all_ids, atlas_release_prop,
        forge, subject_prop, brain_location_prop, reference_system_prop, contribution,
        base_derivation, resource_tag, True, True, volume_path_id_only)
    volume_orig_content = json.loads(open(volume_path_all_ids).read())
    assert volume_orig_content == volume_id_only_content

    volume_path_no_id = os.path.join(folder, "cellCompVolume_noId.json")
    volume_path_id_only = volume_path_no_id.replace(".json", "_id_only.json")
    volume_id_only_content = register_densities(volume_path_no_id, atlas_release_prop,
        forge, subject_prop, brain_location_prop, reference_system_prop, contribution,
        base_derivation, resource_tag, True, True, volume_path_id_only)
    volume_orig_content = json.loads(open(volume_path_no_id).read())

    mts_orig = volume_orig_content[part_key]
    mts = volume_id_only_content[part_key]
    # loop over original volume
    for mt_orig in mts_orig:
        mt_orig_label = mt_orig["label"]
        ets_orig = mt_orig[part_key]
        for et_orig in ets_orig:
            et_orig_label = et_orig["label"]
            et_orig_part = et_orig[part_key][0]
            # loop over modified volume
            for mt in mts:
                if mt["label"] == mt_orig_label:
                    ets = mt[part_key]
                    for et in ets:
                        if et["label"] == et_orig_label:
                            et_part = et[part_key][0]
                            # assert
                            if et_orig_part.get(id_key):
                                assert et_orig_part == et_part
                            else:
                                assert id_key in et_part
