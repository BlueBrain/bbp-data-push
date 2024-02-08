import logging
from pathlib import Path

from kgforge.specializations.resources import Dataset

from bba_data_push.push_brainmesh import create_mesh_resources
import bba_data_push.commons as comm

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_mesh_resources(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_prop, subject_prop, brain_location_prop, reference_system_prop,
    contribution, base_derivation):

    # Arguments
    dataset_path = [
        str(Path(TEST_PATH, "tests/tests_data/brain_region_mesh/997.obj")),
        str(Path(TEST_PATH, "tests/tests_data/brain_region_mesh/614454384.obj")),
    ]

    hierarchy_path = Path(TEST_PATH, "tests/tests_data/mba_hierarchy.json")
    region_map = comm.get_region_map(hierarchy_path)

    dataset_type = comm.brainMeshType

    resources = create_mesh_resources(
        dataset_path,
        dataset_type,
        region_map,
        atlas_release_prop,
        forge,
        subject_prop,
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
        forge.validate(res, execute_actions_before=True, type_=dataset_type)
