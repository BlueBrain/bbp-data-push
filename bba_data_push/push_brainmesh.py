"""
Create a 'Mesh' , an 'atlasRelease' and an 'ontology' resource payload to push into
Nexus. If the resources already exist in Nexus, they will be fetched and updated
instead.
This script has been designed to function with brain region meshes generated by the
Atlas pipeline.
To know more about 'Mesh' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation:
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import yaml
import json
import fnmatch
from uuid import uuid4
from kgforge.core import Resource
from kgforge.specializations.stores.demo_store import DemoStore
from bba_data_push.commons import (
    get_brain_region_prop,
    get_hierarchy_file,
    return_contribution,
    return_atlasrelease,
    return_activity_payload,
    return_file_hash,
    fetch_linked_resources,
)
import bba_data_push.constants as const

from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_brainmesh.log")


def create_mesh_resources(
    forge,
    inputpath: list,
    config_path,
    atlasrelease_config_path,
    input_hierarchy: list,
    input_hierarchy_jsonld,
    provenance_metadata_path,
    link_regions_path,
    resource_tag,
    verbose,
) -> list:
    """
    Construct the input brain mesh dataset, atlasrelease and hierarchy payloads that
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
        link_regions_path : Json file meant to contain the @ids of the brain regions
                    masks, meshes and region summaries.
        resource_tag : Tag value (string).
        verbose : Verbosity level.
    Returns:
        resources_payloads : dict of the form containing the Resource objects
                (volumetricdatalayer, atlasrelease, hierarchy, activity) that has been
                constructed and need to be updated/pushed in Nexus.
    """
    L.setLevel(verbose)

    # Constructs the payloads schema according to the 2 different possible mesh
    # dataset to be pushed
    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        meshes = config_content["GeneratedDatasetPath"]["MeshFile"]
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

    # Dict containing all the pipeline generated mesh datasets and their informations
    try:
        mesh_dict = const.return_mesh_dict(meshes)
    except KeyError as error:
        L.error(f"{error}")
        exit(1)

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
            f"{const.schema_mesh}": [],
            f"{const.schema_atlasrelease}": [],
            f"{const.schema_ontology}": [],
        },
        "datasets_toPush": {
            f"{const.schema_mesh}": [],
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
        flat_tree = {}
        link_summary_content = {}
        fetched_resources = None
        differentAtlasrelease = False
        toUpdate = False
        for dataset in mesh_dict:
            if os.path.isdir(filepath):
                directory = filepath
                files = os.listdir(directory)
                pattern = "*.obj"
                files_mesh = fnmatch.filter(files, pattern)
                if not files_mesh:
                    L.error(f"Error: '{filepath}' do not contain any .obj mesh files")
                    exit(1)
                try:
                    if os.path.samefile(filepath, dataset):
                        fileFound = True
                        mesh_type = [
                            "BrainParcellationMesh",
                            "Mesh",
                            const.dataset_type,
                        ]
                        hierarchy_tag = mesh_dict[dataset]["hierarchy_tag"]
                        annotation_name = mesh_dict[dataset]["annotation_name"]
                        atlasrelease_choice = mesh_dict[dataset]["atlasrelease"]
                        annotation_description = mesh_dict[dataset]["description"]
                except FileNotFoundError:
                    pass
            else:
                L.error(
                    f"Error: '{filepath}' is not a directory. The input dataset need "
                    "to be a directory containing OBJ brain meshes."
                )
                exit(1)
        # If still no file found at this step then raise error
        if not fileFound:
            L.error(
                f"Error: The '{filepath}' folder do not correspond to one of "
                "the brain meshes folder dataset defined in the MeshFile "
                "Section of the 'generated dataset' configuration file"
            )
            exit(1)

        # We create a 1st payload which will serve as template for the others
        meshpath = os.path.join(directory, files_mesh[0])
        try:
            region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
        except ValueError as error:
            L.error(
                f"ValueError in '{meshpath}' file name. {error}. The mesh file names "
                "have to be integer representing their region"
            )
            exit(1)
        file_extension = os.path.splitext(os.path.basename(meshpath))[1][1:]

        try:
            hierarchy_path = get_hierarchy_file(
                input_hierarchy, config_content, hierarchy_tag
            )
            region_name, hierarchy_tree = get_brain_region_prop(
                region_id, ["name"], hierarchy_path, flat_tree
            )
            region_name = region_name["name"]
            flat_tree = hierarchy_tree
        except KeyError as e:
            L.error(f"KeyError: {e}")
            exit(1)
        except ValueError as e:
            L.error(f"ValueError: {e}")
            exit(1)

        L.info(f"Creating the Mesh payload for region {region_id}...")

        # We create a 1st payload that will be recycled in case of multiple files to
        # push

        brainLocation = {
            "brainRegion": {"@id": f"mba:{region_id}", "label": region_name},
            "atlasSpatialReferenceSystem": {
                "@type": [
                    "BrainAtlasSpatialReferenceSystem",
                    "AtlasSpatialReferenceSystem",
                ],
                "@id": const.atlas_reference_system_id,
            },
        }
        mesh_description = (
            f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for the "
            f"{annotation_description}."
        )

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

                # Build the distribution dict with the input hierarchy file
                format_hierarchy_original = os.path.splitext(
                    os.path.basename(atlasrelease_ontology_path)
                )[1][1:]
                content_type_original = f"application/{format_hierarchy_original}"
                hierarchy_original_hash = return_file_hash(atlasrelease_ontology_path)
                input_hierarchy_distrib = {
                    f"{content_type_original}": (
                        hierarchy_original_hash,
                        atlasrelease_ontology_path,
                    )
                }
                # If the correct hierarchy jsonld file is given in input then add it to
                # the distribution dict.
                try:
                    hierarchy_mba = const.atlasrelease_dict[atlasrelease_choice][
                        "ontology"
                    ]["mba_jsonld"]
                    if os.path.samefile(
                        input_hierarchy_jsonld,
                        hierarchies[hierarchy_mba],
                    ):
                        format_hierarchy_mba = os.path.splitext(
                            os.path.basename(input_hierarchy_jsonld)
                        )[1][1:]
                        content_type_mba = f"application/{format_hierarchy_mba}"
                        hierarchy_mba_hash = return_file_hash(input_hierarchy_jsonld)
                        hierarchy_mba_dict = {
                            f"{content_type_mba}": (
                                hierarchy_mba_hash,
                                input_hierarchy_jsonld,
                            )
                        }
                        input_hierarchy_distrib.update(hierarchy_mba_dict)
                except FileNotFoundError as error:
                    L.error(
                        f"Error : {error}. Input hierarchy jsonLD file "
                        "does not correspond to the input hierarchy "
                        "json file"
                    )
                    exit(1)

                # If the hierarchy file has been fetched then the distribution will be
                # updated with the one given in input only if it is different from the
                # ones from the distribution dict. For a brand new file, the
                # distribution will be attached by default.
                distribution_file = []
                if atlasrelease_payloads["hierarchy"].distribution:
                    # Compare the fetched hierarchy file hash with the hash from
                    # the input ones
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
                else:
                    # If the hierarchy file is new so it does not have a distribution
                    # then attach the distribution from the input files
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
                    [mesh_type],
                    [],
                    "isRegionMesh",
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

        # =========================== 1st Payload creation ===========================
        # We create a 1st payload that will be recycled in case of multiple files to
        # push

        # If the resource has been fetched, we compare its distribution to the input
        # file, copy its id and _store_metadata
        if fetched_resources:
            filepath_hash = return_file_hash(meshpath)
            try:
                first_fetched_resource = fetched_resources[f"{region_id}"]
                print(f"first resource: {first_fetched_resource._store_metadata}")
                toUpdate = True
            except KeyError:
                pass
            try:
                if filepath_hash != first_fetched_resource.distribution.digest.value:
                    content_type = f"application/{file_extension}"
                    distribution_file = forge.attach(meshpath, content_type)
            except AttributeError:
                content_type = f"application/{file_extension}"
                distribution_file = forge.attach(meshpath, content_type)
            fetched_resource_id = first_fetched_resource.id
            fetched_resource_metadata = first_fetched_resource._store_metadata
        else:
            content_type = f"application/{file_extension}"
            distribution_file = forge.attach(meshpath, content_type)

        mesh_resource = Resource(
            type=mesh_type,
            name=f"{region_name.title()} Mesh {annotation_name}",
            description=mesh_description,
            atlasRelease=atlasRelease,
            brainLocation=brainLocation,
            distribution=distribution_file,
            isRegisteredIn=const.isRegisteredIn,
            spatialUnit=const.SPATIAL_UNIT,
            subject=const.subject,
            contribution=contribution,
        )
        # dataset = Dataset.from_resource(forge, mesh_resource, store_metadata=True)

        if generation:
            mesh_resource.generation = generation

        # ======================= Create the derivation prop =======================

        if link_regions_path:
            mesh_id = forge.format("identifier", "brainparcellationmesh", str(uuid4()))
            mesh_resource.id = mesh_id
            mesh_link = {"mesh": {"@id": mesh_id}}
            try:
                with open(link_regions_path, "r+") as link_summary_file:
                    link_summary_file.seek(0)
                    link_summary_content = json.loads(link_summary_file.read())
                    # new_summary_file = True
                    try:
                        mask_id = link_summary_content[f"{region_id}"]["mask"]["@id"]
                    except KeyError as error:
                        L.error(
                            f"{error}. The input link region json file need to "
                            "contains the region volumetric Resource Mask @id"
                        )
                        exit(1)
                    mesh_resource.derivation = {
                        "@type": "Derivation",
                        "entity": {
                            "@id": f"{mask_id}",
                            "@type": const.mesh_dict["derivation_type"],
                        },
                    }
                try:
                    if "mesh" not in link_summary_content[f"{region_id}"].keys():
                        link_summary_content[f"{region_id}"].update(mesh_link)
                    else:
                        link_summary_content[f"{region_id}"]["mesh"] = mesh_link["mesh"]
                except KeyError as error:
                    L.error(
                        f"KeyError: {error} is missing in  found in the input link "
                        "region json file"
                    )
                    exit(1)
            except json.decoder.JSONDecodeError as error:
                L.error(
                    f"{error} when opening the input link region json file. it "
                    "need to be created first with the CLI push-volumetric"
                )
                exit(1)
                # region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                # link_summary_content.update(region_summary)
            except FileNotFoundError as error:
                L.error(
                    f"{error} when opening the input link region json file. it "
                    "need to be created first with the CLI push-volumetric"
                )
                exit(1)
                # region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                # link_summary_content.update(region_summary)

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
                mesh_resource.id = fetched_resource_id
            if fetched_resource_metadata:
                mesh_resource._store_metadata = fetched_resource_metadata
            resources_payloads["datasets_toUpdate"][f"{const.schema_mesh}"].append(
                mesh_resource
            )
        else:
            resources_payloads["datasets_toPush"][f"{const.schema_mesh}"].append(
                mesh_resource
            )

        # =================== Construct the rest of the resources ===================

        toUpdate = False
        for f in range(1, len(files_mesh)):  # start at the 2nd file

            meshpath = os.path.join(directory, files_mesh[f])
            try:
                region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
            except ValueError as error:
                L.error(
                    f"ValueError in {'meshpath'} file name. {error}. The mesh file "
                    "names have to be integer representing their region"
                )
                exit(1)
            try:
                region_name, hierarchy_tree = get_brain_region_prop(
                    region_id, ["name"], hierarchy_path, flat_tree
                )
                region_name = region_name["name"]
            except KeyError as e:
                L.error(f"KeyError: {e}")
                exit(1)
            L.info(f"Creating the Mesh payload for region {region_id}...")
            distribution_file = forge.attach(meshpath, content_type)
            brainLocation = {
                "brainRegion": {"@id": f"mba:{region_id}", "label": region_name},
                "atlasSpatialReferenceSystem": {
                    "@type": [
                        "BrainAtlasSpatialReferenceSystem",
                        "AtlasSpatialReferenceSystem",
                    ],
                    "@id": const.atlas_reference_system_id,
                },
            }
            mesh_description = (
                f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for "
                f"the {annotation_description}."
            )

            if fetched_resources:
                try:
                    fetched_resource_id = fetched_resources[f"{region_id}"].id
                    fetched_resource_metadata = fetched_resources[
                        f"{region_id}"
                    ]._store_metadata
                    toUpdate = True
                    filepath_hash = return_file_hash(filepath)
                    try:
                        if (
                            filepath_hash
                            != fetched_resources[
                                f"{region_id}"
                            ].distribution.digest.value
                        ):
                            content_type = fetched_resources[
                                f"{region_id}"
                            ].distribution.encodingFormat
                            distribution_file = forge.attach(
                                fetched_resources[f"{region_id}"].distribution,
                                content_type,
                            )
                    except AttributeError:
                        content_type = fetched_resources[
                            f"{region_id}"
                        ].distribution.encodingFormat
                        distribution_file = forge.attach(
                            fetched_resources[f"{region_id}"].distribution,
                            content_type,
                        )
                except KeyError:
                    pass

            mesh_resources = Resource(
                type=mesh_resource.type,
                name=f"{region_name.title()} Mesh {annotation_name}",
                description=mesh_description,
                atlasRelease=mesh_resource.atlasRelease,
                isRegisteredIn=mesh_resource.isRegisteredIn,
                brainLocation=brainLocation,
                spatialUnit=mesh_resource.spatialUnit,
                distribution=distribution_file,
                contribution=mesh_resource.contribution,
                subject=mesh_resource.subject,
            )

            if generation:
                mesh_resources.generation = mesh_resource.generation

            if link_summary_content:
                mesh_id = forge.format(
                    "identifier", "brainparcellationmesh", str(uuid4())
                )
                mesh_resources.id = mesh_id
                mesh_link = {"mesh": {"@id": mesh_id}}
                # if new_summary_file:
                try:
                    mask_id = link_summary_content[f"{region_id}"]["mask"]["@id"]
                except KeyError as error:
                    L.error(
                        f"{error}. The input link region json file need to "
                        "contains the region volumetric Resource Mask @id"
                    )
                    exit(1)
                mesh_resources.derivation = {
                    "@type": "Derivation",
                    "entity": {
                        "@id": f"{mask_id}",
                        "@type": [
                            "VolumetricDataLayer",
                            "BrainParcellationMask",
                            "Dataset",
                        ],
                    },
                }
                try:
                    print(link_summary_content)
                    if "mesh" not in link_summary_content[f"{region_id}"].keys():
                        link_summary_content[f"{region_id}"].update(mesh_link)
                    else:
                        link_summary_content[f"{region_id}"]["mesh"] = mesh_link["mesh"]
                except KeyError as error:
                    L.error(
                        f"KeyError: The region whose region id is {error} "
                        "can not be found in the input link region json file"
                    )
                    exit(1)
                # else:
                #     region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                #     link_summary_content.update(region_summary)
                if f == len(files_mesh) - 1:
                    link_summary_file = open(link_regions_path, "w")
                    link_summary_file.write(
                        json.dumps(link_summary_content, ensure_ascii=False, indent=2)
                    )
                    link_summary_file.close()

            # dataset = Dataset.from_resource(forge, mesh_resources,
            # store_metadata=True)

            if toUpdate:
                if fetched_resource_id:
                    mesh_resources.id = fetched_resource_id
                if fetched_resource_metadata:
                    mesh_resources._store_metadata = fetched_resource_metadata
                resources_payloads["datasets_toUpdate"][f"{const.schema_mesh}"].append(
                    mesh_resources
                )
            else:
                resources_payloads["datasets_toPush"][f"{const.schema_mesh}"].append(
                    mesh_resources
                )

    resources_payloads["activity"] = activity_resource

    return resources_payloads
