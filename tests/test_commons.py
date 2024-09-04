import pytest
from pathlib import Path

from bba_data_push.bba_dataset_push import get_region_prop
import bba_data_push.commons as comm

from kgforge.core import Resource

TEST_PATH = Path(Path(__file__).parent.parent)


def test_get_region_prop(brain_region_id):
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy_l23split.json"))
    region_prop = get_region_prop(hierarchy_path, brain_region_id)

    assert region_prop == Resource(id=brain_region_id, label="root")


def test_identical_sha():
    local_file_path = Path(TEST_PATH, "tests/tests_data/hierarchy.json")
    remote_file_sha = "2df5228c5cb4c84f9a2fc02e4af9d0aa5cfafe4ee0fbfa6a8f254f84081ba09d"
    assert comm.identical_SHA(local_file_path, remote_file_sha)


def test_get_voxel_type():

    voxel_type = "intensity"
    component_size = 1

    assert comm.get_voxel_type(voxel_type, component_size) == "intensity"

    voxel_type = "vector"
    component_size = 10

    assert comm.get_voxel_type(voxel_type, component_size) == "vector"

    voxel_type = "vector"
    component_size = 1

    with pytest.raises(ValueError) as e:
        comm.get_voxel_type(voxel_type, component_size)
    assert (
        "incompatibility between the provided type (vector) and the component size "
        "(1)" in str(e.value)
    )

    voxel_type = "vector"
    component_size = -5.0

    with pytest.raises(ValueError) as e:
        comm.get_voxel_type(voxel_type, component_size)
    assert ("incompatibility between the provided type (vector) and the component size "
        "(-5.0)" in str(e.value))

    voxel_type = "wrong_voxel_type"
    component_size = 1

    with pytest.raises(KeyError) as e:
        comm.get_voxel_type(voxel_type, component_size)
    assert "'wrong_voxel_type'" in str(e.value)
