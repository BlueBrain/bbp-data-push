import logging
import json
from pathlib import Path

from kgforge.core import Resource
from kgforge.specializations.resources import Dataset

from bba_data_push.commons_new import return_contribution
from bba_data_push.push_nrrd_volumetricdatalayer_new import create_volumetric_resources
from bba_data_push.bba_dataset_push_new import get_existing_resources, get_subject_prop, ATLASRELEASE_TYPE, get_property_type, get_brain_location_prop

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_volumetric_resources(forge, nexus_bucket, nexus_token, nexus_env, atlas_release_id, species_id,
                                     brain_region_id, reference_system_id):
    # Arguments
    dataset_path = [
        str(Path(TEST_PATH, "tests/tests_data/L1_NGC-SA-cNAC.nrrd")),
    ]
    dataset_type = "METypeDensity"

    atlas_release_prop = get_property_type(atlas_release_id, ATLASRELEASE_TYPE)

    species_prop = {"@id": species_id, "label": "Mus Musculus"}
    subject = get_subject_prop(species_prop)

    brain_region_prop = {"@id": brain_region_id, "label": "root"}
    reference_system_prop = {"@id": reference_system_id,
                             "@type": ['AtlasSpatialReferenceSystem',
                                       'BrainAtlasSpatialReferenceSystem']}
    brain_location = {
        "brainRegion": brain_region_prop,
        "atlasSpatialReferenceSystem": reference_system_prop}
    brain_location_prop = get_brain_location_prop(brain_location, reference_system_prop)

    contribution, _ = return_contribution(forge, nexus_bucket, nexus_token,
                                          organization="staging" not in nexus_env)
    base_derivation = {
        "@type": "Derivation",
        "entity": {
            "@id": atlas_release_id,
            "@type": "Entity"}
    }

    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        subject,
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


def test_get_existing_resources(forge, atlas_release_id):
    with open(Path(TEST_PATH, "tests/tests_data/local_ME_density.json")) as local_res_file:
        local_res = json.loads(local_res_file.read())

    res_type = "METypeDensity"

    orig_ress, _ = get_existing_resources(res_type, atlas_release_id, Resource.from_json(local_res), forge, 100)
    assert isinstance(orig_ress, list)
