import pytest
import copy
from pathlib import Path
from kgforge.core import KnowledgeGraphForge

# from bba_dataset_push.bba_data_push import push_cellrecords
from bba_data_push.push_sonata_cellrecordseries import create_cell_record_resources
import bba_data_push.constants as const

TEST_PATH = Path(Path(__file__).parent.parent)


def test_create_cell_record_resources():

    # Arguments
    forge_config_file = str(
        Path(TEST_PATH, "tests/test_forge_config/test_forge_config_demo.yaml")
    )
    nexus_token_file = str(Path(TEST_PATH, "tests/test_forge_config/empty_token.txt"))

    forge = KnowledgeGraphForge(forge_config_file, token=nexus_token_file)

    dataset_path = str(Path(TEST_PATH, "tests/tests_data/cell_records_sonata.h5"))
    config_path = str(Path(TEST_PATH, "tests/tests_data/test_push_dataset_config.yaml"))

    # Arguments wrong
    empty_folder = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/cell_records_sonata")
    )
    corrupted_dataset = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/corrupted_cellrecords.h5")
    )
    wrong_dataset = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/wrong_dataset_cellrecords.h5")
    )
    nocelltype_data = str(
        Path(TEST_PATH, "tests/tests_data/wrong_data/no_celltype_cellrecords.h5")
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
    config_data_notfound = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "dataNotfound_push_dataset_config.yaml",
        )
    )
    config_data_corrupted = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "corruptedData_push_dataset_config.yaml",
        )
    )
    config_data_wrongdataset = str(
        Path(
            TEST_PATH,
            "tests/tests_data/wrong_data/wrong_data_config_file/"
            "wrongdataset_push_dataset_config.yaml",
        )
    )

    cellrecords_resource_simple = {
        "type": ["CellRecordSeries", "Dataset"],
        "atlasRelease": {"@id": const.atlasrelease_hybrid_id},
        "brainLocation": {
            "atlasSpatialReferenceSystem": {
                "@id": const.atlas_reference_system_id,
                "@type": [
                    "BrainAtlasSpatialReferenceSystem",
                    "AtlasSpatialReferenceSystem",
                ],
            },
            "brainRegion": {"label": "root", "@id": "mba:997"},
        },
        "bufferEncoding": "binary",
        "contribution": [],
        "subject": const.subject,
        "description": (
            f"Sonata .h5 file storing the 3D cell positions and orientations of the "
            f"{const.description_hybrid}."
        ),
        "isRegisteredIn": const.isRegisteredIn,
        "name": "Cell Records Sonata",
        "numberOfRecords": {"@type": "xsd:long", "@value": 11},
        "recordMeasure": [
            {
                "label": {
                    "0": "astrocyte",
                    "1": "excitatory_neuron",
                    "2": "inhibitory_neuron",
                    "3": "microglia",
                    "4": "oligodendrocyte",
                },
                "@type": "RecordMeasure",
                "componentEncoding": "uint32",
                "description": "Label of the cell type",
                "name": "cell_type",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Component w of the cell orientation quaternion",
                "name": "orientation_w",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Component x of the cell orientation quaternion",
                "name": "orientation_x",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Component y of the cell orientation quaternion",
                "name": "orientation_y",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Component z of the cell orientation quaternion",
                "name": "orientation_z",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "uint32",
                "description": "Region identifiers (AIBS Structure IDs)",
                "name": "region_id",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Cell position along the X axis",
                "name": "x",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Cell position along the Y axis",
                "name": "y",
            },
            {
                "@type": "RecordMeasure",
                "componentEncoding": "float32",
                "description": "Cell position along the Z axis",
                "name": "z",
            },
        ],
    }

    result = vars(
        create_cell_record_resources(
            forge,
            [dataset_path],
            config_path,
            provenance_metadata_path=None,
            verbose=0,
        )["datasets"][-1]
    )

    for key in cellrecords_resource_simple:
        assert result[key] == cellrecords_resource_simple[key]

    # test with every arguments
    cellrecords_resource_fulloptions = copy.deepcopy(cellrecords_resource_simple)

    result = create_cell_record_resources(
        forge,
        [dataset_path],
        config_path,
        provenance_metadata_path=None,
        verbose=0,
    )["datasets"]

    # Search for the cell_record_dataset to compare with (if multiple results returned)
    cell_record_dataset = None
    for dataset in result:
        if vars(dataset)["name"] == "Cell Records Sonata":
            cell_record_dataset = vars(dataset)

    for key in cellrecords_resource_fulloptions:
        assert cell_record_dataset[key] == cellrecords_resource_fulloptions[key]

    # Check every exceptions :

    # configuration file with wrong keys
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            dataset_path,
            wrong_config_key,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset corrupted
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            [corrupted_dataset],
            config_data_corrupted,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset with wrong content
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            [wrong_dataset],
            config_data_wrongdataset,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # configuration file contains not existing file path
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            [dataset_path],
            config_data_notfound,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset is an empty folder
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            [empty_folder],
            config_wrongdatatype,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1

    # dataset without cell_type content
    with pytest.raises(SystemExit) as e:
        create_cell_record_resources(
            forge,
            [nocelltype_data],
            config_data_emptydata,
            provenance_metadata_path=None,
            verbose=0,
        )[-1]
    assert e.value.code == 1


def test_push_cellrecords():
    # Wait for a future Nexus token management. But will probably be an integration test
    pass
