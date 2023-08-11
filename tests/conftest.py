import os
import pytest
from kgforge.core import KnowledgeGraphForge

from bba_data_push.bba_dataset_push import (REFSYSTEM_TYPE, get_property_type,
    get_subject_prop, get_brain_location_prop)
import bba_data_push.commons as comm


@pytest.fixture
def nexus_env():
    return "https://staging.nise.bbp.epfl.ch/nexus/v1"


@pytest.fixture
def forge_config_file():
    return "./forge_configuration/forge_config.yml"


@pytest.fixture
def nexus_org():
    return "bbp"


@pytest.fixture
def nexus_proj():
    return "atlas"


@pytest.fixture
def nexus_bucket(nexus_org, nexus_proj):
    return "/".join([nexus_org, nexus_proj])


@pytest.fixture
def nexus_token():
    return os.environ["NEXUS_STAGING_TOKEN"]


@pytest.fixture
def forge(nexus_env, forge_config_file, nexus_org, nexus_proj, nexus_token):
    return KnowledgeGraphForge(forge_config_file, endpoint=nexus_env, bucket="/".join([nexus_org, nexus_proj]),
                               token=nexus_token)


@pytest.fixture
def atlas_release_id():
    return "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease/c96c71a8-4c0d-4bc1-8a1a-141d9ed6693d"


@pytest.fixture
def atlas_release_prop(atlas_release_id):
    return get_property_type(atlas_release_id, comm.all_types[comm.atlasrelaseType])


@pytest.fixture
def cell_composition_id():
    return "https://bbp.epfl.ch/neurosciencegraph/data/cellcompositions/54818e46-cf8c-4bd6-9b68-34dffbc8a68c"


@pytest.fixture
def brain_region_id():
    return "http://api.brain-map.org/api/v2/data/Structure/997"


@pytest.fixture
def brain_region_prop(brain_region_id):
    return {"@id": brain_region_id, "label": "root"}


@pytest.fixture
def reference_system_id():
    return "https://bbp.epfl.ch/neurosciencegraph/data/allen_ccfv3_spatial_reference_system"


@pytest.fixture
def reference_system_prop(reference_system_id):
    return get_property_type(reference_system_id, REFSYSTEM_TYPE)


@pytest.fixture
def species_id():
    return "http://purl.obolibrary.org/obo/NCBITaxon_10090"


@pytest.fixture
def species_prop(species_id):
    return {"@id": species_id, "label": "Mus Musculus"}


@pytest.fixture
def brain_location_prop(brain_region_prop, reference_system_prop):
    return get_brain_location_prop(brain_region_prop, reference_system_prop)


@pytest.fixture()
def subject_prop(species_prop):
    return get_subject_prop(species_prop)


@pytest.fixture
def brain_template_id():
    return "https://bbp.epfl.ch/neurosciencegraph/data/dca40f99-b494-4d2c-9a2f-c407180138b7"


@pytest.fixture
def brain_template_id():
    return "https://bbp.epfl.ch/neurosciencegraph/data/dca40f99-b494-4d2c-9a2f-c407180138b7"


@pytest.fixture
def base_derivation(atlas_release_id):
    return {
        "@type": "Derivation",
        "entity": {
            "@id": atlas_release_id,
            "@type": "Entity"}}
