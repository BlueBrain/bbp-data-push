import pytest
from pathlib import Path

from bba_data_push.deprecated_commons import (
    get_voxel_type,
    get_hierarchy_file,
    get_brain_region_prop,
    # return_contribution,
)

from bba_data_push.bba_dataset_push import get_region_prop
import bba_data_push.commons as comm

from kgforge.core import Resource

TEST_PATH = Path(Path(__file__).parent.parent)


def test_get_region_prop(brain_region_id):
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy_l23split.json"))
    region_prop = get_region_prop(hierarchy_path, brain_region_id)

    assert region_prop == Resource(id=brain_region_id, label="root")


def test_identical_SHA():
    local_file_path = Path(TEST_PATH, "tests/tests_data/hierarchy.json")
    remote_file_sha = "2df5228c5cb4c84f9a2fc02e4af9d0aa5cfafe4ee0fbfa6a8f254f84081ba09d"
    assert comm.identical_SHA(local_file_path, remote_file_sha)

def test_get_voxel_type():

    voxel_type = "intensity"
    component_size = 1

    assert get_voxel_type(voxel_type, component_size) == "intensity"

    voxel_type = "vector"
    component_size = 10

    assert get_voxel_type(voxel_type, component_size) == "vector"

    voxel_type = "vector"
    component_size = 1

    with pytest.raises(ValueError) as e:
        get_voxel_type(voxel_type, component_size)
    assert (
        "incompatibility between the provided type (vector) and the component size "
        "(1)" in str(e.value)
    )

    voxel_type = "vector"
    component_size = -5.0

    with pytest.raises(ValueError) as e:
        get_voxel_type(voxel_type, component_size)
    assert (
        "incompatibility between the provided type (vector) and the component size "
        "(-5.0)" in str(e.value)
    )

    voxel_type = "wrong_voxel_type"
    component_size = 1

    with pytest.raises(KeyError) as e:
        get_voxel_type(voxel_type, component_size)
    assert "'wrong_voxel_type'" in str(e.value)


def test_get_hierarchy_file():

    input_hierarchy = [str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))]
    config_content = {
        "hierarchyJson": ["hierarchy", "hierarchy_l23split"],
        "HierarchyJson": {
            "hierarchy": "tests/tests_data/hierarchy.json",
            "hierarchy_l23split": "tests/tests_data/hierarchy_l23split.json",
        },
    }
    hierarchy_tag = "hierarchy"

    assert get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag) == str(
        Path(TEST_PATH, "tests/tests_data/hierarchy.json")
    )

    input_hierarchy = [
        str(Path(TEST_PATH, "tests/tests_data/hierarchy.json")),
        str(Path(TEST_PATH, "tests/tests_data/hierarchy_l23split.json")),
    ]
    config_content = {
        "hierarchyJson": ["hierarchy", "hierarchy_l23split"],
        "HierarchyJson": {
            "hierarchy": "tests/tests_data/hierarchy.json",
            "hierarchy_l23split": "tests/tests_data/hierarchy_l23split.json",
        },
    }
    hierarchy_tag = "hierarchy_l23split"

    assert get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag) == str(
        Path(TEST_PATH, "tests/tests_data/hierarchy_l23split.json")
    )

    input_hierarchy = [
        str(Path(TEST_PATH, "tests/tests_data/wrong_data/empty_hierarchy.json"))
    ]
    config_content = {
        "hierarchyJson": ["hierarchy", "hierarchy_l23split"],
        "HierarchyJson": {
            "hierarchy": "tests/tests_data/hierarchy.json",
            "hierarchy_l23split": "tests/tests_data/hierarchy_l23split.json",
        },
    }
    hierarchy_tag = "hierarchy"

    with pytest.raises(KeyError) as e:
        get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag)
    assert "The right hierarchy file is not among those given as input" in str(e.value)

    input_hierarchy = [str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))]
    config_content = {
        "hierarchyJson": ["hierarchy", "hierarchy_l23split"],
        "HierarchyJson": {
            "hierarchy": "tests/tests_data/hierarchy.json",
            "hierarchy_l23split": "tests/tests_data/hierarchy_l23split.json",
        },
    }
    hierarchy_tag = "hierarchy_l23split"

    with pytest.raises(KeyError) as e:
        get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag)
    assert "The right hierarchy file is not among those given as input" in str(e.value)

    input_hierarchy = [str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))]
    config_content = {"wrong_key": "wrong_value"}
    hierarchy_tag = "hierarchy"

    with pytest.raises(KeyError) as e:
        get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag)
    assert "HierarchyJson" in str(e.value)

    input_hierarchy = [str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))]
    config_content = {
        "hierarchyJson": ["hierarchy", "hierarchy_l23split"],
        "HierarchyJson": {
            "hierarchy": "tests/tests_data/hierarchy.json",
            "hierarchy_l23split": "tests/tests_data/hierarchy_l23split.json",
        },
    }
    hierarchy_tag = "wrong_hierarchy_tag"

    with pytest.raises(KeyError) as e:
        get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag)
    assert "wrong_hierarchy_tag" in str(e.value)


def test_get_brain_region_name():

    region_id = 1
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))
    flat_tree = None

    region_name, hierarchy = get_brain_region_prop(
        region_id, ["name"], hierarchy_path, flat_tree
    )
    region_name = region_name["name"]

    assert region_name == "region_1"
    assert hierarchy == {
        1: {"acronym": "end", "children": [], "id": 1, "name": "region_1"},
        8: {
            "acronym": "grey",
            "children": [1],
            "id": 8,
            "name": "Basic cell groups and regions",
        },
        998: {"children": [8], "id": 998, "name": "root"},
    }

    region_id = 8
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))
    flat_tree = hierarchy

    assert (
        get_brain_region_prop(region_id, ["name"], hierarchy_path, flat_tree)[0]["name"]
        == "Basic cell groups and regions"
    )

    region_id = "wrong_region_id"
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))
    flat_tree = None

    with pytest.raises(ValueError) as e:
        get_brain_region_prop(region_id, ["name"], hierarchy_path, flat_tree)
    assert "ValueError: invalid literal for int() with base 10" in str(e.value)

    region_id = 0
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))
    flat_tree = hierarchy

    with pytest.raises(KeyError) as e:
        get_brain_region_prop(region_id, ["name"], hierarchy_path, flat_tree)
    assert "Region name corresponding to id '0' is not found" in str(e.value)

    region_id = 1
    hierarchy_path = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/empty_hierarchy.json")
    )
    flat_tree = None

    with pytest.raises(ValueError) as e:
        get_brain_region_prop(region_id, ["name"], hierarchy_path, flat_tree)
    assert "Error when decoding the hierarchy json file" in str(e.value)

    region_id = 1
    hierarchy_path = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrongkey_hierarchy.json")
    )
    flat_tree = None

    with pytest.raises(KeyError) as e:
        get_brain_region_prop(region_id, ["name"], hierarchy_path, flat_tree)
    assert "Region name corresponding to id '1' is not found" in str(e.value)


def test_return_contribution():
    # Wait for a future Nexus token management.
    pass
