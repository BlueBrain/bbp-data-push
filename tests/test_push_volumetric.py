import logging
import json
from pathlib import Path

from kgforge.core import Resource
from kgforge.specializations.resources import Dataset

from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources
import bba_data_push.commons as comm

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_volumetric_resources(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_prop, subject_prop, brain_location_prop, reference_system_prop,
    contribution, base_derivation):

    # Arguments
    dataset_path = (
        str(Path(TEST_PATH, "tests/tests_data/L1_NGC-SA-cNAC.nrrd")),
    )
    dataset_type = comm.meTypeDensity

    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        subject_prop,
        brain_location_prop,
        reference_system_prop,
        contribution,
        base_derivation,
        L
    )

    assert type(resources) == list
    assert len(resources) == len(dataset_path)
    for res in resources:
        assert type(res) == Dataset
        assert dataset_type in res.type
        forge.validate(res, execute_actions_before=True, type_="NeuronDensity")


def test_create_volumetric_ph(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_prop, subject_prop, brain_location_prop, reference_system_prop,
    contribution, base_derivation):
    # Arguments
    dataset_path = (
        str(Path(TEST_PATH, "tests/tests_data/placement_hints")),
    )
    dataset_type = comm.placementHintsType

    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        subject_prop,
        brain_location_prop,
        reference_system_prop,
        contribution,
        base_derivation,
        L
    )

    assert type(resources) == list
    for res in resources:
        assert type(res) == Dataset
        assert dataset_type in res.type


def test_create_volumetric_mask(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_prop, subject_prop, reference_system_prop, contribution,
    base_derivation):

    # Arguments
    dataset_path = (
        str(Path(TEST_PATH, "tests/tests_data/brain_region_mask")),
    )
    dataset_type = comm.brainMaskType

    hierarchy_path = Path(TEST_PATH, "tests/tests_data/mba_hierarchy.json")
    flat_tree = comm.get_flat_tree(hierarchy_path)

    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        subject_prop,
        None,
        reference_system_prop,
        contribution,
        base_derivation,
        L,
        None,
        flat_tree
    )

    assert type(resources) == list
    for res in resources:
        assert type(res) == Dataset
        assert dataset_type in res.type
        forge.validate(res, execute_actions_before=True, type_=dataset_type)


def test_get_existing_resources(forge, atlas_release_id):
    with open(Path(TEST_PATH, "tests/tests_data/local_ME_density.json")) as local_res_file:
        local_res = json.loads(local_res_file.read())

    res_type = comm.meTypeDensity

    orig_ress, _ = comm.get_existing_resources(res_type, atlas_release_id, Resource.from_json(local_res), forge, 100)
    assert isinstance(orig_ress, list)
