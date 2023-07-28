import os
import logging
from pathlib import Path
from kgforge.core import KnowledgeGraphForge
from kgforge.core import Resource

from bba_data_push.commons_new import return_contribution

from bba_data_push.push_nrrd_volumetricdatalayer_new import create_volumetric_resources

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_volumetric_resources():
    # Arguments
    forge_config_file = "./forge_configuration/forge_config.yml"

    nexus_env = "https://staging.nise.bbp.epfl.ch/nexus/v1"
    nexus_org = "bbp"
    nexus_proj = "atlas"
    nexus_token = os.environ["NEXUS_STAGING_TOKEN"]

    bucket = "/".join([nexus_org, nexus_proj])
    forge = KnowledgeGraphForge(forge_config_file, endpoint=nexus_env, bucket=bucket,
                                token=nexus_token)

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
        contribution
    )

    assert type(resources) == list
    assert len(resources) == len(dataset_path)
    for res in resources:
        assert type(res) == Resource
        forge.validate(res, execute_actions_before=True, type_="NeuronDensity")
