"""
Create a 'CellRecordSeries', an 'atlasRelease' and an 'ontology' resource payload to 
push into Nexus. If the resources already exist in Nexus, they will be fetched and 
updated instead. 
This script has been designed to function with sonata h5 files storing 3D brain cell 
positions and orientations and generated by the Atlas pipeline.
To know more about 'Mesh' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: 
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import json
import yaml
import h5py
from kgforge.core import Resource
from kgforge.specializations.stores.demo_store import DemoStore

from bba_data_push.commons import (
    return_contribution,
    get_hierarchy_file,
    return_activity_payload,
    return_atlasrelease,
    return_file_hash,
    fetch_linked_resources,
)
import bba_data_push.constants as const
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_cellrecord.log")


def create_cell_record_resources(
    forge,
    inputpath: list,
    config_path,
    atlasrelease_config_path,
    input_hierarchy: list,
    input_hierarchy_jsonld,
    provenance_metadata_path,
    resource_tag,
    verbose,
) -> list:
    """
    Construct the input sonata hdf5 dataset, atlasrelease and hierarchy payloads that
    will be push with the corresponding files into Nexus as a resource.

    Parameters:
        forge : instantiated and configured forge object.
        inputpath : input datasets paths. These datasets are either volumetric files
                    or folder containing volumetric files.
        config_path : configuration yaml file path containing the names and paths of
                    the Atlas Annotation pipeline generated datasets.
        atlasrelease_config_path : Json file containing the atlasRelease @id as well
                    as its ontology and parcellation volume @id. It needs to contains
                    at least these informations for the atlasRelease Allen Mouse CCFV2
                    and CCFV3 stocked in the Nexus project bbp/atlas.
        input_hierarchy : hierarchy json files.
        input_hierarchy_jsonld : hierarchy jsonld file to be attached to the
                    atlasrelease ontology.
        provenance_metadata_path : configuration json file containing various
                    information about dataset provenance generated from the Atlas
                    Annotation Pipeline run.
        resource_tag : Tag value (string).
        verbose : Verbosity level.
    Returns:
        resources_payloads : dict of the form containing the Resource objects
                (volumetricdatalayer, atlasrelease, hierarchy, activity) that has been
                constructed and need to be updated/pushed in Nexus.
    """
    L.setLevel(verbose)

    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        cellrecords = config_content["GeneratedDatasetPath"]["CellRecordsFile"]
        hierarchies = config_content["HierarchyJson"]
    except KeyError as error:
        L.error(f"KeyError: {error} is not found in the dataset configuration file.")
        exit(1)

    if provenance_metadata_path:
        try:
            with open(provenance_metadata_path, "r") as f:
                provenance_metadata = json.loads(f.read())
        except ValueError as error:
            L.error(f"{error} : {provenance_metadata_path}.")
            exit(1)
    else:
        provenance_metadata = None

    # Dict containing all the pipeline generated cellrecord datasets and their
    # informations
    try:
        cellrecords_dict = const.return_cellrecords_dict(cellrecords)
    except KeyError as error:
        L.error(f"{error}")
        exit(1)

    # Constructs the Resource properties payloads accordingly to the input atlas cell
    # record datasets
    Measures_table = {
        "x": "Cell position along the X axis",
        "y": "Cell position along the Y axis",
        "z": "Cell position along the Z axis",
        "orientation_w": "Component w of the cell orientation quaternion",
        "orientation_x": "Component x of the cell orientation quaternion",
        "orientation_y": "Component y of the cell orientation quaternion",
        "orientation_z": "Component z of the cell orientation quaternion",
        "cell_type": "Label of the cell type",
        "region_id": "Region identifiers (AIBS Structure IDs)",
    }

    brainLocation = {
        "brainRegion": {"@id": "mba:997", "label": "root"},
        "atlasSpatialReferenceSystem": {
            "@type": [
                "BrainAtlasSpatialReferenceSystem",
                "AtlasSpatialReferenceSystem",
            ],
            "@id": const.atlas_reference_system_id,
        },
    }

    # Create contribution
    if isinstance(forge._store, DemoStore):
        contribution = []
    else:
        try:
            contribution, log_info = return_contribution(forge)
            L.info("\n".join(log_info))
        except Exception as e:
            L.error(f"Error: {e}")
            exit(1)

    # Constants

    resources_payloads = {
        "datasets_toUpdate": {
            f"{const.schema_cellrecord}": [],
            f"{const.schema_atlasrelease}": [],
            f"{const.schema_ontology}": [],
        },
        "datasets_toPush": {
            f"{const.schema_cellrecord}": [],
            f"{const.schema_atlasrelease}": [],
            f"{const.schema_ontology}": [],
        },
        "activity": [],
        "tag": "",
    }
    atlasrelease_payloads = {
        "atlasrelease_choice": None,
        "hierarchy": False,
        "tag": None,
        "fetched": False,
    }
    atlasrelease_choosen = []
    atlasRelease = {}
    generation = {}
    activity_resource = []
    for filepath in inputpath:
        fileFound = False
        fetched_resources = None
        differentAtlasrelease = False
        toUpdate = False
        for dataset in cellrecords_dict:
            try:
                if os.path.samefile(filepath, dataset):
                    fileFound = True
                    if filepath.endswith(".h5"):
                        filename_noext = os.path.splitext(os.path.basename(filepath))[0]
                        cellrecord_type = ["CellRecordSeries", const.dataset_type]
                        description = cellrecords_dict[dataset]["description"]
                        atlasrelease_choice = cellrecords_dict[dataset]["atlasrelease"]
                    else:
                        L.error(
                            f"Error: cell-record sonata dataset '{filepath}' is not a "
                            "sonata .h5 file"
                        )
                        exit(1)
                else:
                    L.error(
                        f"Error: The '{filepath}' folder do not correspond to a Sonata "
                        ".h5 file dataset defined in the CellPositionFile section of "
                        "the input datasets configuration file."
                    )
                    exit(1)
            except FileNotFoundError:
                pass
        # If still no file found at this step then raise error
        if not fileFound:
            L.error(
                f"FileNotFoundError: '{filepath}' file do not correspond to one of "
                "the cellrecords sonata dataset defined in the CellRecordsFile Section "
                "of the 'generated dataset' configuration file"
            )
            exit(1)

        # TO REMOVE IN THE FUTUR AND USE THE ATLASRELEASE CONFIGURATION FILE INSTEAD:
        if isinstance(atlasrelease_choice, dict):
            atlasRelease = atlasrelease_choice

        # =========================== Payload creation ===========================

        try:
            cell_collections = h5py.File(filepath, "r")
        except OSError as e:
            L.error(f"OSError when trying to open the input file {filepath}. {e}")
            L.info("Aborting pushing process.")  # setLevel(logging.INFO)
            exit(1)

        recordMeasure = []
        try:
            sonata_datasets = cell_collections["nodes"]["atlas_cells"]["0"]
            for sonata_dataset in sonata_datasets.keys():
                if sonata_dataset in Measures_table:
                    Measure_payload = {
                        "@type": "RecordMeasure",
                        "description": Measures_table[sonata_dataset],
                        "componentEncoding": f"{sonata_datasets[sonata_dataset].dtype}",
                        "name": f"{sonata_dataset}",
                    }
                    if sonata_dataset == "cell_type":
                        cell_types = sonata_datasets["@library"]["cell_type"]
                        if all(isinstance(x, bytes) for x in cell_types):
                            cell_types = [s.decode("UTF-8") for s in cell_types]
                        elif any(isinstance(x, bytes) for x in cell_types):
                            L.error(
                                "ValueError: @library/cell_type contains string and "
                                "bytes (literal string). The content need to be uniform"
                            )
                            exit(1)
                        Measure_payload["label"] = {
                            f"{i}": cell_types[i] for i in range(0, len(cell_types))
                        }
                        # labels
                    recordMeasure.append(Measure_payload)
        except KeyError as e:
            L.error(
                f"KeyError during the information extraction of the dataset in the "
                f"input file {filepath}. {e}"
            )
            exit(1)

        try:
            numberOfRecords = {
                "@type": "xsd:long",
                "@value": cell_collections.get("/nodes/atlas_cells/0/x").shape[0],
            }
        except KeyError as e:
            L.error(
                f"KeyError during the information extraction of the dataset in the "
                f"input file {filepath}. {e}"
            )
            exit(1)

        # ======= Fetch the atlasRelease Resource linked to the input datasets =======

        if not isinstance(forge._store, DemoStore):
            # Check that the same atlasrelease is not treated again
            if not atlasrelease_payloads["atlasrelease_choice"] or (
                atlasrelease_choice not in atlasrelease_choosen
            ):
                differentAtlasrelease = True
                atlasrelease_choosen.append(atlasrelease_choice)
                atlasrelease_payloads["atlasrelease_choice"] = atlasrelease_choice
                try:
                    atlasrelease_payloads = return_atlasrelease(
                        forge,
                        atlasrelease_config_path,
                        atlasrelease_payloads,
                        resource_tag,
                    )
                    if atlasrelease_payloads["fetched"]:
                        L.info("atlasrelease Resource found in the Nexus project")
                    else:
                        L.error(
                            "atlasrelease Resource not found in the Nexus project. "
                            "You need to first create it and push it into Nexus "
                            "using the CLI push-volumetric"
                        )
                        exit(1)
                except Exception as e:
                    L.error(f"Exception: {e}")
                    exit(1)
                except AttributeError as e:
                    L.error(f"AttributeError: {e}")
                    exit(1)

                atlasRelease = {
                    "@id": atlasrelease_payloads["atlas_release"].id,
                    "@type": atlasrelease_payloads["atlas_release"].type,
                }

                resources_payloads["tag"] = atlasrelease_payloads["tag"]

                # ========= Check that the atlas Ontology is present in input =========

                # For a new atlas release creation verify first that the right
                # parcellation volume and hierarchy file have been provided and attach
                # the distribution. For an update, compare first if they distribution
                # are different before attaching it

                # => check if the good hierarchy file is given in input
                try:
                    atlasrelease_ontology_path = get_hierarchy_file(
                        input_hierarchy,
                        config_content,
                        const.atlasrelease_dict[atlasrelease_choice]["ontology"][
                            "name"
                        ],
                    )
                except KeyError:
                    try:
                        # If it is an update
                        if atlasrelease_payloads["hierarchy"].distribution:
                            pass
                    # Then it is a brand new creation and the file is needed
                    except AttributeError as error:
                        L.error(
                            "Error: the ontology file corresponding to the "
                            "created atlasRelease resource can not be found among "
                            f"input hierarchy files. {error}"
                        )
                        exit(1)

                # If the hierarchy file has been fetched then the distribution will be
                # updated with the one given in input only if it is different. For a
                # brand new file, the distribution will be attached.
                try:
                    if atlasrelease_payloads["hierarchy"].distribution:
                        format_hierarchy_original = os.path.splitext(
                            os.path.basename(atlasrelease_ontology_path)
                        )[1][1:]
                        content_type_original = (
                            f"application/{format_hierarchy_original}"
                        )
                        hierarchy_original_hash = return_file_hash(
                            atlasrelease_ontology_path
                        )
                        input_hierarchy_distrib = {
                            f"{content_type_original}": (
                                hierarchy_original_hash,
                                atlasrelease_ontology_path,
                            )
                        }
                        # If the correct hierarchy jsonld file is given in input then
                        # compare it and eventually attach it for the ontology resource
                        # distribution as well
                        try:
                            if (
                                input_hierarchy_jsonld
                                and const.atlasrelease_dict[atlasrelease_choice][
                                    "ontology"
                                ]["mba_jsonld"]
                            ):
                                hierarchy_jsonld_name = const.atlasrelease_dict[
                                    atlasrelease_choice
                                ]["ontology"]["mba_jsonld"]
                                try:
                                    if os.path.samefile(
                                        input_hierarchy_jsonld,
                                        hierarchies[hierarchy_jsonld_name],
                                    ):
                                        pass
                                except FileNotFoundError as error:
                                    L.error(
                                        f"Error : {error}. Input hierarchy jsonLD file "
                                        "does not correspond to the input hierarchy "
                                        "json file"
                                    )
                                    exit(1)
                                format_hierarchy_mba = os.path.splitext(
                                    os.path.basename(input_hierarchy_jsonld)
                                )[1][1:]
                                content_type_mba = f"application/{format_hierarchy_mba}"
                                hierarchy_mba_hash = return_file_hash(
                                    input_hierarchy_jsonld
                                )
                                hierarchy_mba_dict = {
                                    f"{content_type_mba}": (
                                        hierarchy_mba_hash,
                                        input_hierarchy_jsonld,
                                    )
                                }
                                input_hierarchy_distrib.update(hierarchy_mba_dict)
                        except KeyError:
                            pass

                        # Compare the fetched hierarchy file hash with the hash from
                        # the input ones
                        distribution_file = []
                        if not isinstance(
                            atlasrelease_payloads["hierarchy"].distribution, list
                        ):
                            atlasrelease_payloads["hierarchy"].distribution = [
                                atlasrelease_payloads["hierarchy"].distribution
                            ]
                        for fetched_distrib in atlasrelease_payloads[
                            "hierarchy"
                        ].distribution:
                            try:
                                if (
                                    fetched_distrib.digest.value
                                    != input_hierarchy_distrib[
                                        fetched_distrib.encodingFormat
                                    ][0]
                                ):
                                    distribution_hierarchy = forge.attach(
                                        input_hierarchy_distrib[
                                            fetched_distrib.encodingFormat
                                        ][1],
                                        fetched_distrib.encodingFormat,
                                    )
                                    # attach the selected input distribution and pop it
                                    # from the dictionary
                                    distribution_file.append(distribution_hierarchy)
                                    input_hierarchy_distrib.pop(
                                        fetched_distrib.encodingFormat
                                    )
                            except KeyError:
                                pass
                            # If still keys in it then attach the remaining files
                            if input_hierarchy_distrib:
                                print("ccc")
                                for encoding, file in input_hierarchy_distrib.items():
                                    distribution_hierarchy = forge.attach(
                                        file[1],
                                        encoding,
                                    )
                                    distribution_file.append(distribution_hierarchy)
                except AttributeError:
                    # If the hierarchy file is new so it does not have a distribution
                    for encoding, file in input_hierarchy_distrib.items():
                        distribution_hierarchy = forge.attach(file[1], encoding)
                        distribution_file.append(distribution_hierarchy)

                atlasrelease_payloads["hierarchy"].distribution = distribution_file

                # ==================== Link atlasRelease/Ontology ====================
                if not atlasrelease_payloads["atlas_release"].parcellationOntology:
                    atlasrelease_payloads["atlas_release"].parcellationOntology = {
                        "@id": atlasrelease_payloads["hierarchy"].id,
                        "@type": ["Entity", const.ontology_type, "Ontology"],
                    }
                atlasrelease_payloads["hierarchy"].contribution = contribution

                resources_payloads["datasets_toUpdate"][
                    f"{const.schema_ontology}"
                ].append(atlasrelease_payloads["hierarchy"])

        # ==================== Fetch atlasRelease linked resources ====================

        if atlasrelease_payloads["fetched"]:
            try:
                L.info(
                    "Resources in Nexus which correspond to input datasets will be "
                    "updated..."
                )
                # fetched_resources will be either one resource or a dictionary of
                # resource
                fetched_resources = fetch_linked_resources(
                    forge,
                    atlasrelease_payloads,
                    [cellrecord_type],
                    [],
                    "isCellRecord",
                )
            except KeyError as error:
                L.error(f"{error}")
                exit(1)
            except IndexError as error:
                L.error(f"{error}")
                exit(1)

        # ==================== add Activity and generation prop ====================

        if provenance_metadata and not activity_resource:
            try:
                activity_resource = return_activity_payload(forge, provenance_metadata)
            except Exception as e:
                L.error(f"{e}")
                exit(1)

            # if the activity Resource has been fetched from Nexus, the property
            # 'value' need to be mapped back to @value
            if hasattr(activity_resource, "startedAtTime"):
                # A Resource property that is a dict at the creation of the Resource
                # become a Resource attribut after being synchronized on Nexus
                if not isinstance(activity_resource.startedAtTime, dict):
                    if hasattr(activity_resource.startedAtTime, "@value"):
                        value = getattr(activity_resource.startedAtTime, "@value")
                        activity_resource.startedAtTime = forge.from_json(
                            {
                                "type": activity_resource.startedAtTime.type,
                                "@value": value,
                            }
                        )

            generation = {
                "@type": "Generation",
                "activity": {
                    "@id": activity_resource.id,
                    "@type": activity_resource.type,
                },
            }

        # ============================ Payload creation ============================

        # If the resource has been fetched, we compare its distribution to the input
        # file, copy its id and _store_metadata
        if fetched_resources:
            filepath_hash = return_file_hash(filepath)
            first_fetched_resource = fetched_resources
            toUpdate = True
            print(f"first resource: {first_fetched_resource._store_metadata}")
            try:
                if filepath_hash != first_fetched_resource.distribution.digest.value:
                    distribution_file = forge.attach(filepath)
            except AttributeError:
                distribution_file = forge.attach(filepath)
            fetched_resource_id = first_fetched_resource.id
            fetched_resource_metadata = first_fetched_resource._store_metadata
        else:
            distribution_file = forge.attach(filepath)

        # add personalised content_type = "application/" + extension (according to
        # mime convention)

        cellrecord_resource = Resource(
            type=cellrecord_type,
            name=filename_noext.replace("_", " ").title(),
            distribution_file=distribution_file,
            description=description,
            atlasRelease=atlasRelease,
            isRegisteredIn=const.isRegisteredIn,
            brainLocation=brainLocation,
            recordMeasure=recordMeasure,
            numberOfRecords=numberOfRecords,
            bufferEncoding="binary",
            subject=const.subject,
            contribution=contribution,
        )
        # resource.fileExtension = config["file_extension"]
        # dataset = Dataset.from_resource(forge, cellrecord_resource,
        # store_metadata=True) #several dataset = datasets

        # dataSampleModality=["parcellationId", "cellTypeId", "position3D",
        # "eulerAngle"],

        if generation:
            cellrecord_resource.generation = generation

        # Add the generation prop for every different atlasRelease
        if differentAtlasrelease:
            if generation:
                atlasrelease_payloads["hierarchy"].generation = generation
                atlasrelease_payloads["atlas_release"].generation = generation
            resources_payloads["datasets_toUpdate"][
                f"{const.schema_atlasrelease}"
            ].append(atlasrelease_payloads["atlas_release"])

        if toUpdate:
            if fetched_resource_id:
                cellrecord_resource.id = fetched_resource_id
            if fetched_resource_metadata:
                cellrecord_resource._store_metadata = fetched_resource_metadata
            resources_payloads["datasets_toUpdate"][
                f"{const.schema_cellrecord}"
            ].append(cellrecord_resource)
        else:
            resources_payloads["datasets_toPush"][f"{const.schema_cellrecord}"].append(
                cellrecord_resource
            )

    resources_payloads["activity"] = activity_resource

    return resources_payloads
