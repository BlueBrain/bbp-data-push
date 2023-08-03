import os
import logging
import json
from pathlib import Path
from copy import deepcopy

from kgforge.core import KnowledgeGraphForge
from kgforge.core import Resource

from bba_data_push.commons_new import return_contribution
from bba_data_push.push_nrrd_volumetricdatalayer_new import create_volumetric_resources
from bba_data_push.bba_dataset_push_new import match_properties, TypePropertiesMap

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

TEST_PATH = Path(Path(__file__).parent.parent)

forge_config_file = "./forge_configuration/forge_config.yml"

nexus_env = "https://staging.nise.bbp.epfl.ch/nexus/v1"
nexus_org = "bbp"
nexus_proj = "atlas"
nexus_token = os.environ["NEXUS_STAGING_TOKEN"]

bucket = "/".join([nexus_org, nexus_proj])
forge = KnowledgeGraphForge(forge_config_file, endpoint=nexus_env, bucket=bucket, token=nexus_token)


def test_create_volumetric_resources():
    # Arguments
    dataset_path = [
        str(Path(TEST_PATH, "tests/tests_data/L1_NGC-SA.nrrd")),
    ]
    dataset_type = "METypeDensity"
    atlas_release_id = "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease" \
                       "/c96c71a8-4c0d-4bc1-8a1a-141d9ed6693d"
    species_id = "http://purl.obolibrary.org/obo/NCBITaxon_10090"
    brain_region_id = "http://api.brain-map.org/api/v2/data/Structure/997"
    reference_system_id = "https://bbp.epfl.ch/neurosciencegraph/data" \
                          "/allen_ccfv3_spatial_reference_system"

    atlas_release_prop = {"@id": atlas_release_id,
                          "@type": ["AtlasRelease", "BrainAtlasRelease"]}
    species_prop = {"@id": species_id, "label": "Mus Musculus"}
    brain_region_prop = {"@id": brain_region_id, "label": "root"}
    reference_system_prop = {"@id": reference_system_id,
                             "@type": ['AtlasSpatialReferenceSystem',
                                       'BrainAtlasSpatialReferenceSystem']}
    contribution, _ = return_contribution(forge, bucket, nexus_token,
                                          organization="staging" not in nexus_env)

    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        species_prop,
        brain_region_prop,
        reference_system_prop,
        contribution,
        L
    )

    assert type(resources) == list
    assert len(resources) == len(dataset_path)
    for res in resources:
        assert type(res) == Resource
        assert dataset_type in res.type
        forge.validate(res, execute_actions_before=True, type_="NeuronDensity")


def test_match_properties():
    # fetch res from Nexus, remove distribution and dump resource json into a file local_res.json
    with open(str(Path(TEST_PATH, "tests/tests_data/local_ME_density.json"))) as local_res_file:
        local_res = json.loads(local_res_file.read())
    # fetch res from Nexus abd dump resource json into a filw existing_res.json
    with open(Path(TEST_PATH, "tests/tests_data/orig_ME_density.json")) as existing_res_file:
        existing_res = json.loads(existing_res_file.read())

    res_type = "METypeDensity"
    properties = TypePropertiesMap.prop[res_type]

    res_id, _ = match_properties(local_res, [Resource.from_json(existing_res)], forge, properties)
    assert res_id == existing_res["id"]

    for prop in properties:
        existing_res_wrong = deepcopy(existing_res)
        existing_res_wrong[prop] = None
        res_id, _ = match_properties(local_res, [Resource.from_json(existing_res_wrong)], forge, properties)
        assert res_id is None