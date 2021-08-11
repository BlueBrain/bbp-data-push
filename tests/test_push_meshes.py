import pytest
import copy
from pathlib import Path
from kgforge.core import KnowledgeGraphForge

#from bba_dataset_push.bba_data_push import push_meshes
from bba_dataset_push.push_brainmesh import create_mesh_resources

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_mesh_resources():

    # Arguments
    forge_config_file = str(
        Path(TEST_PATH, "tests/test_forge_config/test_forge_config_demo.yaml")
    )
    nexus_token_file = str(Path(TEST_PATH, "tests/test_forge_config/empty_token.txt"))

    forge = KnowledgeGraphForge(forge_config_file, token=nexus_token_file)

    dataset_path = [
        str(Path(TEST_PATH, "tests/tests_data/brain_region_meshes_hybrid")),
        str(Path(TEST_PATH, "tests/tests_data/brain_region_meshes_l23split")),
    ]
    config_path = str(Path(TEST_PATH, "tests/tests_data/test_push_dataset_config.yaml"))
    hierarchy_path = [
        str(Path(TEST_PATH, "tests/tests_data/hierarchy.json")),
        str(Path(TEST_PATH, "tests/tests_data/hierarchy_l23split.json")),
    ]
    provenance = "parcellation2mesh:parcellation2mesh, version 0.0.1"

    # Arguments wrong
    empty_folder = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/empty_brain_region_meshes_hybrid")
    )
    wrong_dataset_name = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrong_mesh_dataset_name")
    )
    not_a_dir = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/brain_region_meshes_hybrid/1.obj")
    )
    wrong_config_key = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrongkey_push_dataset_config.yaml")
    )

    config_wrongdatatype = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "wrongdatatype_push_dataset_config.yaml",
        )
    )
    config_data_emptydata = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "emptydata_push_dataset_config.yaml",
        )
    )
    config_data_emptyhierarchy = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "emptyHierarchy_push_dataset_config.yaml",
        )
    )
    config_data_wronghierarchy = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "wrongHierarchy_push_dataset_config.yaml",
        )
    )
    config_data_notfound = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "dataNotfound_push_dataset_config.yaml",
        )
    )

    empty_hierarchy = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/empty_hierarchy.json")
    )
    wrong_hierarchy = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrongkey_hierarchy.json")
    )
    wrong_provenance = "wrong_provenance"

    mesh_resource_simple = {
        "type": "BrainParcellationMesh",
        "atlasRelease": {
            "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
            "e2e500ec-fe7e-4888-88b9-b72425315dda"
        },
        "brainLocation": {
            "atlasSpatialReferenceSystem": {
                "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
                "allen_ccfv3_spatial_reference_system",
                "@type": [
                    "BrainAtlasSpatialReferenceSystem",
                    "AtlasSpatialReferenceSystem",
                ],
            },
            "brainRegion": {"label": "region_1", "@id": "mba:1"},
        },
        "contribution": [],
        "description": "Brain region mesh - Region_1 (ID: 1). It is based in the "
        "parcellation volume resulting of the hybridation between CCFv2 and CCFv3.",
        "distribution": "",
        "isRegisteredIn": {
            "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
            "allen_ccfv3_spatial_reference_system",
            "@type": [
                "BrainAtlasSpatialReferenceSystem",
                "AtlasSpatialReferenceSystem",
            ],
        },
        "name": "Region_1 Mesh",
        "spatialUnit": "µm",
    }

    result = vars(
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            config_path,
            [hierarchy_path[0]],
            provenances=[None],
            verbose=0,
        )[-1]
    )

    for key in mesh_resource_simple:
        if key != "distribution":
            assert result[key] == mesh_resource_simple[key]

    # test with every arguments
    mesh_resource_fulloptions = copy.deepcopy(mesh_resource_simple)
    mesh_resource_fulloptions[
        "description"
    ] = "Brain region mesh - Region_1 (ID: 1). It is based in the parcellation volume "
    "resulting of the hybridation between CCFv2 and CCFv3. Generated in the Atlas "
    "Pipeline by the module 'parcellation2mesh' version 0.0.1."

    result = vars(
        create_mesh_resources(
            forge,
            dataset_path,
            config_path,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=1,
        )[-1]
    )

    for key in mesh_resource_fulloptions:
        if key != "distribution":
            assert result[key] == mesh_resource_fulloptions[key]

    # Check every exceptions :

    # configuration file with wrong keys
    with pytest.raises(KeyError) as e:
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            wrong_config_key,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert str(e.value) == "'brain_region_meshes_hybrid'"

    # dataset with wrong name
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [wrong_dataset_name],
            config_path,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset is not a directory
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [not_a_dir],
            config_wrongdatatype,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # configuration file contains not existing file path
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            config_data_notfound,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset is an empty folder
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [empty_folder],
            config_data_emptydata,
            [hierarchy_path[0]],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # hierarchy file is empty
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            config_data_emptyhierarchy,
            [empty_hierarchy],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # hierarchy file do not contain some regions
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            config_data_wronghierarchy,
            [wrong_hierarchy],
            provenances=[provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # provenance is wrong
    with pytest.raises(SystemExit) as e:
        create_mesh_resources(
            forge,
            [dataset_path[0]],
            config_path,
            [hierarchy_path[0]],
            provenances=[wrong_provenance],
            verbose=0,
        )[-1]
    assert e.value.code == 1


def test_push_meshes():
    # Wait for a future Nexus token management. But will probably be an integration test
    pass
