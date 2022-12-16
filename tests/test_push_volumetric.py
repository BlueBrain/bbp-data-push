import pytest
import numpy as np
from pathlib import Path
from kgforge.core import KnowledgeGraphForge
from kgforge.core import Resource

# from bba_dataset_push.bba_data_push import push_volumetric
from bba_data_push.push_nrrd_volumetricdatalayer import (
    create_volumetric_resources,
    add_nrrd_props,
)
import bba_data_push.constants as const

TEST_PATH = Path(Path(__file__).parent.parent)


def volumetric_dict(cell_density=False, nrrd_props=False):

    volumetric_dict = {
        "type": ["Dataset", "VolumetricDataLayer", "BrainParcellationDataLayer"],
        "atlasRelease": {},
        "brainLocation": {
            "atlasSpatialReferenceSystem": {
                "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
                "allen_ccfv3_spatial_reference_system",
                "@type": [
                    "BrainAtlasSpatialReferenceSystem",
                    "AtlasSpatialReferenceSystem",
                ],
            },
            "brainRegion": {"label": "root", "@id": "mba:997"},
        },
        "contribution": [],
        "subject": {
            "@type": "Subject",
            "species": {
                "@id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
                "label": "Mus musculus",
            },
        },
        "description": "Hybrid annotation volume from ccfv2 and ccfv3 at 25 um"
        "The version replaces the leaf regions in ccfv3 with the leaf region of ccfv2, "
        "which have additional levels of hierarchy.",
        "isRegisteredIn": {
            "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
            "allen_ccfv3_spatial_reference_system",
            "@type": [
                "BrainAtlasSpatialReferenceSystem",
                "AtlasSpatialReferenceSystem",
            ],
        },
        "name": "Annotation V2V3 Hybrid",
        "componentEncoding": "float64",
    }

    if nrrd_props:

        volumetric_dict.update(
            {
                "bufferEncoding": "gzip",
                "endianness": "little",
                "fileExtension": "nrrd",
                "dimension": [
                    {"@type": "ComponentDimension", "name": "label", "size": 1},
                    {"@type": "SpaceDimension", "size": 1, "unitCode": "um"},
                    {"@type": "SpaceDimension", "size": 2, "unitCode": "um"},
                    {"@type": "SpaceDimension", "size": 3, "unitCode": "um"},
                ],
                "resolution": {"unitCode": "um", "value": 25.0},
                "sampleType": "label",
                "worldMatrix": [
                    25.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    25.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    25.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            }
        )

    if cell_density:

        volumetric_dict["type"].remove("BrainParcellationDataLayer")
        volumetric_dict["type"].extend(["CellDensityDataLayer", "GliaCellDensity"])
        volumetric_dict["name"] = "Excitatory Neuron Density"
        volumetric_dict["sampleType"] = "intensity"
        volumetric_dict["dimension"][0]["name"] = "intensity"

    return volumetric_dict


def test_create_volumetric_resources():

    # Arguments
    forge_config_file = str(
        Path(TEST_PATH, "tests/test_forge_config/test_forge_config_demo.yaml")
    )
    nexus_token_file = str(Path(TEST_PATH, "tests/test_forge_config/empty_token.txt"))

    forge = KnowledgeGraphForge(forge_config_file, token=nexus_token_file)

    dataset_path = [
        str(Path(TEST_PATH, "tests/tests_data/annotation_v2v3_hybrid.nrrd")),
        str(Path(TEST_PATH, "tests/tests_data/annotation_hybrid_l23split.nrrd")),
        str(Path(TEST_PATH, "tests/tests_data/cell_densities_hybrid")),
        str(Path(TEST_PATH, "tests/tests_data/neuron_densities_hybrid")),
    ]
    hierarchy_path = str(Path(TEST_PATH, "tests/tests_data/hierarchy.json"))
    config_path = str(Path(TEST_PATH, "tests/tests_data/test_push_dataset_config.yaml"))
    atlasrelease_config_path = str(
        Path(TEST_PATH, "/tests/tests_data/atlasrelease_config_path.json")
    )
    dataset_returned = "datasets_toPush"
    dataset_schema = const.schema_volumetricdatalayer

    # Arguments wrong
    empty_folder = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/empty_cell_density")
    )
    wrong_dataset_name = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrong_name.nrrd")
    )
    corrupted_data_header = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/corrupted_header.nrrd")
    )
    wrong_config_key = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrongkey_push_dataset_config.yaml")
    )
    folder_annotation = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/folder_annotation")
    )
    neuron_density_file = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/neuron_density_file.nrrd")
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
    config_data_notfound = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "dataNotfound_push_dataset_config.yaml",
        )
    )
    config_corruptedData = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "corruptedData_push_dataset_config.yaml",
        )
    )

    volumetric_dict_simple = volumetric_dict(cell_density=False, nrrd_props=True)

    datasets = create_volumetric_resources(
            forge=forge,
            inputpath=[dataset_path[0]],
            config_path=config_path,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema]
    if len(datasets):
        result = vars(datasets[-1])
        for key in volumetric_dict_simple:
            assert result[key] == volumetric_dict_simple[key]

    # test with every arguments
    cell_density_dict_fulloptions = volumetric_dict(cell_density=True, nrrd_props=True)

    cell_density_dict_fulloptions["description"] = (
        "Excitatory neuron density volume for the Hybrid annotation volume from ccfv2 "
        "and ccfv3 at 25 um."
    )

    result = create_volumetric_resources(
        forge,
        dataset_path,
        config_path,
        new_atlas=False,
        atlasrelease_config_path=atlasrelease_config_path,
        input_hierarchy=hierarchy_path,
        input_hierarchy_jsonld=None,
        provenance_metadata_path=None,
        link_regions_path=None,
        resource_tag=None,
        verbose=1,
    )[dataset_returned][dataset_schema]

    # Search for the excitatory neuron dataset to compare with (if multiple results
    # returned)
    excitatory_neuron_dataset = None
    for dataset in result:
        if vars(dataset)["name"] == "Excitatory Neuron Density":
            excitatory_neuron_dataset = vars(dataset)

    if excitatory_neuron_dataset:
        # result return the payload from only the last dataset processed
        for key in cell_density_dict_fulloptions:
            assert excitatory_neuron_dataset[key] == cell_density_dict_fulloptions[key]

    # Check every exceptions :

    # configuration file with wrong keys
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [dataset_path[0]],
            wrong_config_key,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # dataset is an empty folder
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [empty_folder],
            config_data_emptydata,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # dataset with wrong name
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [wrong_dataset_name],
            config_path,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # configuration file contains not existing file path
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [dataset_path[0]],
            config_data_notfound,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # annotation dataset is a folder
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [folder_annotation],
            config_wrongdatatype,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # cell density dataset is a file
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [neuron_density_file],
            config_wrongdatatype,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1

    # dataset with wrong nrrd header
    with pytest.raises(SystemExit) as e:
        create_volumetric_resources(
            forge,
            [corrupted_data_header],
            config_corruptedData,
            new_atlas=False,
            atlasrelease_config_path=atlasrelease_config_path,
            input_hierarchy=hierarchy_path,
            input_hierarchy_jsonld=None,
            provenance_metadata_path=None,
            link_regions_path=None,
            resource_tag=None,
            verbose=0,
        )[dataset_returned][dataset_schema][-1]
    assert e.value.code == 1


def test_add_nrrd_props():

    # Arguments
    volumetric_dict_simple = volumetric_dict(cell_density=False, nrrd_props=False)
    volumetric_resource = Resource(
        type=volumetric_dict_simple["type"],
        atlasRelease=volumetric_dict_simple["atlasRelease"],
        brainLocation=volumetric_dict_simple["brainLocation"],
        contribution=volumetric_dict_simple["contribution"],
        description=volumetric_dict_simple["description"],
        isRegisteredIn=volumetric_dict_simple["isRegisteredIn"],
        name=volumetric_dict_simple["name"],
        componentEncoding=volumetric_dict_simple["componentEncoding"],
        subject=volumetric_dict_simple["subject"],
    )
    config = {
        "file_extension": "nrrd",
        "sampling_space_unit": "um",
        "sampling_period": 30,
        "sampling_time_unit": "ms",
    }

    nrrd_header = {
        "type": "double",
        "dimension": 3,
        "space": "left-posterior-superior",
        "sizes": np.array([1, 2, 3]),
        "space directions": np.array(
            [[25.0, 0.0, 0.0], [0.0, 25.0, 0.0], [0.0, 0.0, 25.0]]
        ),
        "kinds": ["domain", "domain", "domain"],
        "endian": "little",
        "encoding": "gzip",
        "space origin": np.array([0.0, 0.0, 0.0]),
    }
    correct_voxel_type = "label"
    wrong_voxel_type = "wrong_voxel_type"

    result = vars(
        add_nrrd_props(volumetric_resource, nrrd_header, config, correct_voxel_type)
    )

    volumetric_dict_fulloptions = volumetric_dict(cell_density=False, nrrd_props=True)
    for key in volumetric_dict_fulloptions:
        assert result[key] == volumetric_dict_fulloptions[key]

    # voxel type is wrong
    with pytest.raises(KeyError) as e:
        add_nrrd_props(volumetric_resource, nrrd_header, config, wrong_voxel_type)
    assert "'wrong_voxel_type'" in str(e.value)


def test_push_volumetric():
    # Wait for a future Nexus token management. But will probably be an integration test
    pass
