import os
import logging

from kgforge.core import Resource

from bba_data_push.push_atlas_release import create_atlas_release
from bba_data_push.bba_dataset_push import get_property_type, BRAIN_TEMPLATE_TYPE
import bba_data_push.commons as comm

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def test_create_atas_release(forge, nexus_bucket, nexus_token, nexus_env,
    atlas_release_id, brain_location_prop, reference_system_prop, subject_prop,
    brain_template_id, contribution):

    test_folder = os.environ["TEST_FOLDER"]
    folder = os.path.join(test_folder, "tests_data")

    hierarchy_path = os.path.join(folder, "hierarchy_l23split.json")
    hierarchy_ld_path = os.path.join(folder, "hierarchy_l23split_ld.json")
    annotation_path = os.path.join(folder, "annotation.nrrd")
    hemisphere_path = os.path.join(folder, "hemispheres.nrrd")

    brain_template_prop = get_property_type(brain_template_id, BRAIN_TEMPLATE_TYPE)

    test_id = "dummy-id"
    ont_prop = get_property_type(test_id, comm.ontologyType)
    par_prop = get_property_type(test_id, comm.parcellationType)
    hem_prop = get_property_type(test_id, comm.hemisphereType)
    ph_prop = get_property_type(test_id, comm.placementHintsType)
    name = "AtlasRelease from unit test"
    description = name
    atlas_release_resource = create_atlas_release(atlas_release_id, brain_location_prop,
        reference_system_prop, brain_template_prop, subject_prop, ont_prop,
        par_prop, hem_prop, ph_prop, contribution, name, description)

    assert type(atlas_release_resource) == Resource
    assert comm.atlasrelaseType in atlas_release_resource.type
    forge.validate(atlas_release_resource, execute_actions_before=True)  # check which type_ option can be provided
