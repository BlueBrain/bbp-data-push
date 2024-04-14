import os
import pytest
import logging
import random

from kgforge.core import Resource
from kgforge.core.wrappings.dict import wrap_dict

from bba_data_push.push_atlas_release import create_atlas_release, create_ph_catalog_distribution
from bba_data_push.bba_dataset_push import BRAIN_TEMPLATE_TYPE
import bba_data_push.commons as comm

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def test_create_atlas_release(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_id, brain_location_prop, reference_system_prop, subject_prop,
    brain_template_id, contribution):
    brain_template_prop = comm.get_property_type(brain_template_id, BRAIN_TEMPLATE_TYPE)

    test_id = "dummy-id"
    ont_prop = comm.get_property_type(test_id, comm.ontologyType)
    par_prop = comm.get_property_type(test_id, comm.parcellationType)
    hem_prop = comm.get_property_type(test_id, comm.hemisphereType)
    ph_prop = comm.get_property_type(test_id, comm.placementHintsType)
    dv_prop = comm.get_property_type(test_id, comm.directionVectorsType)
    co_prop = comm.get_property_type(test_id, comm.cellOrientationType)
    name = "AtlasRelease from unit test"
    description = name
    atlas_release_resource = create_atlas_release(atlas_release_id, brain_location_prop,
        reference_system_prop, brain_template_prop, subject_prop, ont_prop,
        par_prop, hem_prop, ph_prop, dv_prop, co_prop, contribution, name, description)

    assert type(atlas_release_resource) == Resource
    assert comm.atlasrelaseType in atlas_release_resource.type
    forge.validate(atlas_release_resource, execute_actions_before=True)  # check which type_ option can be provided


def test_create_ph_catalog_distribution(forge):
    brain_region_label = "Isocortex"
    ph_resources, ph_res_to_filepath, filepath_to_brainregion_json = \
        _create_ph_resources(["layer_1", "layer_5", "layer_2", "layer_3", "layer_6", "layer_4"], "file://gpfs/path/%5BPH%5D", brain_region_label)

    ph_y = Resource(id="https://ph_y",
                    distribution=Resource(
                        atLocation=Resource.from_json({"location": "file://gpfs/path/%5BPH%5Dy.nrrd"}),
                        name="[PH]y.nrrd"))
    ph_y._store_metadata = wrap_dict({"_rev": 1})
    ph_mask = Resource(id="https://Isocortex_problematic_voxel_mask",
                       distribution=Resource(
                           atLocation=Resource.from_json({"location": "file://gpfs/path/Isocortex_problematic_voxel_mask.nrrd"}),
                           name="Isocortex_problematic_voxel_mask.nrrd"))
    ph_mask._store_metadata = wrap_dict({"_rev": 1})
    ph_resources.append(ph_y)
    ph_resources.append(ph_mask)

    ph_res_to_filepath[ph_y.get_identifier()] = "./test_data/placement_hints/[PH]y.nrrd"
    ph_res_to_filepath[ph_mask.get_identifier()] = "./test_data/placement_hints/Isocortex_problematic_voxel_mask.nrrd"

    filepath_to_brainregion_json[os.path.basename("./test_data/placement_hints/[PH]y.nrrd")] = [brain_region_label]
    filepath_to_brainregion_json[os.path.basename("./test_data/placement_hints/Isocortex_problematic_voxel_mask.nrrd")] = [brain_region_label]

    random.shuffle(ph_resources)
    ph_catalog_distribution = create_ph_catalog_distribution(ph_resources,
        filepath_to_brainregion_json, ph_res_to_filepath, forge, debug=True)

    assert isinstance(ph_catalog_distribution, dict)
    assert len(ph_catalog_distribution) == 2
    assert "placementHints" in ph_catalog_distribution
    assert len(ph_catalog_distribution["placementHints"]) == len(ph_resources) - 2
    assert "voxelDistanceToRegionBottom" in ph_catalog_distribution
    isocortex_resource = forge.retrieve("http://api.brain-map.org/api/v2/data/Structure/315", cross_bucket=True)
    isocortex_leaf_region_resources = [forge.retrieve(r, cross_bucket=True) for r in isocortex_resource.hasLeafRegionPart]
    isocortex_leaf_region_notations = {r.notation: r for r in isocortex_leaf_region_resources}
    layers = []
    for ph in ph_catalog_distribution["placementHints"]:
        regions = ph["regions"]
        assert len(regions) == 1
        assert brain_region_label in regions
        hasLeafRegionPart = regions[brain_region_label]["hasLeafRegionPart"]
        layer = regions[brain_region_label]["layer"]
        layers.append(layer["label"])
        layers_to_check = [layer["@id"]]
        if layer["@id"] == "http://purl.obolibrary.org/obo/UBERON_0005395":  # layer 6, need to also check layer 6a and layer 6b
            layers_to_check.extend([
                "https://bbp.epfl.ch/ontologies/core/bmo/neocortex_layer_6a",
                "http://purl.obolibrary.org/obo/UBERON_8440003"])
        for leaf_region_notation in hasLeafRegionPart:
            assert leaf_region_notation in isocortex_leaf_region_notations
        isocortex_leaf_region_notations_layer_only = [n for n, r in isocortex_leaf_region_notations.items() if hasattr(r, "hasLayerLocationPhenotype") and any(item in r.hasLayerLocationPhenotype for item in layers_to_check)]
        assert len(isocortex_leaf_region_notations_layer_only) > 0
        isocortex_leaf_region_notations_layer_only.sort()
        hasLeafRegionPart.sort()
        assert isocortex_leaf_region_notations_layer_only == hasLeafRegionPart
    assert layers == ["L1", "L2", "L3", "L4", "L5", "L6"]

    # Check for wrong resource
    wrong_brain_region_label = "TH"  # "Thalamus" region is not in Isocortex layer 1
    ph_resources, ph_res_to_filepath, filepath_to_brainregion_json = _create_ph_resources(["layer_1"], "file://gpfs/path/%5BPH%5D", wrong_brain_region_label)
    with pytest.raises(Exception):
        create_ph_catalog_distribution(ph_resources, filepath_to_brainregion_json, ph_res_to_filepath, forge)


def test_get_resource_rev(forge, atlas_release_id):
    tag_rev_dict = {"leaves-only_atlas": 10,
                    "non-existing-tag": None}
    for tag, rev in tag_rev_dict.items():
        assert rev == comm.get_resource_rev(forge, atlas_release_id, tag)


def _create_ph_resources(layers, location_prefix, brain_region_label):
    ph_resources = []
    ph_res_to_filepath = {}
    filepath_to_brainregion_json = {}
    for layer in layers:
        ph_layer = Resource(
            id=f"https://ph_{layer}",
            distribution=Resource(
                atLocation=Resource.from_json({"location": f"{location_prefix}{layer}.nrrd"}),
                name=f"[PH]{layer}.nrrd"))
        ph_layer._store_metadata = wrap_dict({"_rev": 1})
        ph_resources.append(ph_layer)

        file_path = f"./test_data/placement_hints/[PH]{layer}.nrrd"
        ph_res_to_filepath[ph_layer.get_identifier()] = file_path
        
        filepath_to_brainregion_json[os.path.basename(file_path)] = [brain_region_label]
        
    return ph_resources, ph_res_to_filepath, filepath_to_brainregion_json
