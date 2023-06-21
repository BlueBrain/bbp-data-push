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
import copy
import fnmatch
from uuid import uuid4
from kgforge.core import Resource
from kgforge.specializations.stores.demo_store import DemoStore
from bba_data_push.commons import (
    get_brain_region_prop,
    get_hierarchy_file,
    return_contribution,
    fetch_atlasrelease,
    return_activity_payload,
    return_file_hash,
    fetch_linked_resources,
)
import bba_data_push.constants as const

def create_mesh_resources(
    forge,
    inputpath: list,
    config_path,
    new_atlas,
    atlasrelease_config_path,
    input_hierarchy: list,
    input_hierarchy_jsonld,
    provenance_metadata_path,
    link_regions_path,
    resource_tag,
    logger) -> list:
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
        logger : logger.
    Returns:
        resources_payloads : dict of the form containing the Resource objects
                (volumetricdatalayer, atlasrelease, hierarchy, activity) that has been
                constructed and need to be updated/pushed in Nexus.
    """

    # Constructs the payloads schema according to the 2 different possible mesh
    # dataset to be pushed
    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        meshes = config_content["GeneratedDatasetPath"]["MeshFile"]
        hierarchies = config_content["HierarchyJson"]
    except KeyError as error:
        logger.error(f"KeyError: {error} is not found in the dataset configuration file.")
        exit(1)

    if provenance_metadata_path:
        try:
            with open(provenance_metadata_path, "r") as f:
                provenance_metadata = json.loads(f.read())
        except ValueError as error:
            logger.error(f"{error} : {provenance_metadata_path}.")
            exit(1)
    else:
        provenance_metadata = None

    # Dict containing all the pipeline generated mesh datasets and their informations
    try:
        mesh_dict = const.return_mesh_dict(meshes)
    except KeyError as error:
        logger.error(f"{error}")
        exit(1)

    # Create contribution
    if isinstance(forge._store, DemoStore):
        contribution = []
    else:
        try:
            contribution, log_info = return_contribution(forge)
            logger.info("\n".join(log_info))
        except Exception as e:
            logger.error(f"Error: {e}")
            exit(1)

    # Constants

    resources_payloads = {
        "activity": [],
        "tag": "",
    }
    actions = ["toUpdate","toPush"]
    dataset_structure = {
        f"{const.schema_mesh}": [],
        f"{const.schema_atlasrelease}": [],
        f"{const.schema_ontology}": [],
    }
    for action in actions:
        resources_payloads.update({"datasets_"+action: copy.deepcopy(dataset_structure)})

    atlasrelease_payloads = {
        "atlasrelease_choice": None,
        "atlas_release": {},
        "hierarchy": None,
        "tag": None,
        "fetched": False,
        "aibs_atlasrelease": False,
    }
    generation = {}
    activity_resource = []
    for filepath in inputpath:
        fileFound = False
        flat_tree = {}
        link_summary_content = {}
        distribution_file = None
        fetched_resources = None
        differentAtlasrelease = False
        action = "toPush"
        pushOrUpdate = True
        fetched_resource_id = None
        fetched_resource_metadata = None
        for dataset in mesh_dict:
            if os.path.isdir(filepath):
                directory = filepath
                files = os.listdir(directory)
                pattern = "*.obj"
                files_mesh = fnmatch.filter(files, pattern)
                if not files_mesh:
                    logger.error(f"Error: '{filepath}' do not contain any .obj mesh files")
                    exit(1)
                try:
                    if os.path.samefile(filepath, dataset):
                        fileFound = True
                        mesh_type = mesh_dict[dataset]["type"]
                        hierarchy_tag = mesh_dict[dataset]["hierarchy_tag"]
                        annotation_name = mesh_dict[dataset]["annotation_name"]
                        atlasrelease_choice = mesh_dict[dataset]["atlasrelease"]
                        annotation_description = mesh_dict[dataset]["description"]
                        derivation_type = mesh_dict[dataset]["derivation_type"]
                except FileNotFoundError:
                    pass
            else:
                logger.error(
                    f"Error: '{filepath}' is not a directory. The input dataset need "
                    "to be a directory containing OBJ brain meshes."
                )
                exit(1)
        # If still no file found at this step then raise error
        if not fileFound:
            logger.error(
                f"Error: The '{filepath}' folder do not correspond to one of "
                "the brain meshes folder dataset defined in the MeshFile "
                "Section of the 'generated dataset' configuration file"
            )
            exit(1)

        # We create a 1st payload which will serve as template for the others
        meshpath = os.path.join(directory, files_mesh[0])
        file_extension = os.path.splitext(os.path.basename(meshpath))[1][1:]
        try:
            region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
        except ValueError as error:
            logger.error(
                f"ValueError in '{meshpath}' file name. {error}. The mesh file names "
                "have to be integer representing their region"
            )
            exit(1)

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
            logger.error(f"KeyError: {e}")
            exit(1)
        except ValueError as e:
            logger.error(f"ValueError: {e}")
            exit(1)

        logger.info(f"Creating the Mesh payload for region {region_id}...")

        # We create a 1st payload that will be recycled in case of multiple files to
        # push

        brainLocation = {
            "brainRegion": {"@id": f"mba:{region_id}", "label": region_name},
            "atlasSpatialReferenceSystem": {
                "@type": [
                    "BrainAtlasSpatialReferenceSystem",
                    "AtlasSpatialReferenceSystem",
                ],
                "@id": const.atlas_spatial_reference_system_id,
            },
        }
        mesh_description = (
            f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for the "
            f"{annotation_description}."
        )

        # ======= Fetch the atlasRelease Resource linked to the input datasets =======
        atlasrelease_payloads = fetch_atlasrelease(forge, logger, atlasrelease_config_path, new_atlas, input_hierarchy, input_hierarchy_jsonld, config_content, inputpath, provenance_metadata, contribution, resource_tag, differentAtlasrelease, atlasrelease_payloads, resources_payloads)

        # ==================== Fetch atlasRelease linked resources ====================

        if (
            atlasrelease_payloads["fetched"]
            or atlasrelease_payloads["aibs_atlasrelease"]
        ):
            resource_type_list = list(
                set(mesh_type).difference(set([const.dataset_type, const.mesh_type]))
            )
            try:
                # fetched_resources will be either one resource or a dictionary of
                # resource
                fetched_resources = fetch_linked_resources(
                    forge,
                    atlasrelease_payloads["atlas_release"],
                    resource_type_list,
                    [],
                    "isRegionMesh",
                    parcellationAtlas_id=None,
                )
            except KeyError as error:
                logger.error(f"{error}")
                exit(1)
            except IndexError as error:
                logger.error(f"{error}")
                exit(1)

        # ==================== add Activity and generation prop ====================

        if provenance_metadata and not activity_resource:
            try:
                activity_resource = return_activity_payload(forge, provenance_metadata)
                if not activity_resource._store_metadata:
                    logger.info(
                        "Existing activity resource not found in the Nexus destination "
                        f"project '{forge._store.bucket}'. A new activity will be "
                        "created and registered"
                    )
            except Exception as e:
                logger.error(f"{e}")
                exit(1)

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
        print("1st Payload")
        # If the resource has been fetched, we compare its distribution to the input
        # file, copy its id and _store_metadata
        if fetched_resources:
            filepath_hash = return_file_hash(meshpath)
            first_fetched_resource = None
            if str(region_id) in fetched_resources:
                action = "toUpdate"
                first_fetched_resource = fetched_resources[f"{region_id}"]
                fetched_resource_id = first_fetched_resource.id
                fetched_resource_metadata = first_fetched_resource._store_metadata
                try:
                    if (
                        filepath_hash
                        != first_fetched_resource.distribution.digest.value
                    ):
                        content_type = f"application/{file_extension}"
                        distribution_file = forge.attach(meshpath, content_type)
                    else:
                        pushOrUpdate = False
                        distribution_file = first_fetched_resource.distribution
                # If no distribution in the fetched resources then attach the input file
                except AttributeError:
                    content_type = f"application/{file_extension}"
                    distribution_file = forge.attach(meshpath, content_type)
            else:
                content_type = f"application/{file_extension}"
                distribution_file = forge.attach(meshpath, content_type)

        mesh_resource = Resource(
            type=mesh_type,
            name=f"{region_name.title()} Mesh {annotation_name}",
            description=mesh_description,
            atlasRelease=atlasrelease_payloads["atlas_release"],
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
        if fetched_resource_id:
            mesh_resource.id = fetched_resource_id
        if fetched_resource_metadata:
            mesh_resource._store_metadata = fetched_resource_metadata

        # ======================= Create the derivation prop =======================

        if link_regions_path:
            if hasattr(mesh_resource, "id"):
                mesh_id = mesh_resource.id
            else:
                mesh_id = forge.format(
                    "identifier", "brainparcellationmesh", str(uuid4())
                )
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
                        logger.error(
                            f"{error}. The input link region json file need to "
                            "contains the region volumetric Resource Mask @id"
                        )
                        exit(1)
                    mesh_resource.derivation = {
                        "@type": "Derivation",
                        "entity": {
                            "@id": f"{mask_id}",
                            "@type": derivation_type,
                        },
                    }
                try:
                    if "mesh" not in link_summary_content[f"{region_id}"].keys():
                        link_summary_content[f"{region_id}"].update(mesh_link)
                    else:
                        link_summary_content[f"{region_id}"]["mesh"] = mesh_link["mesh"]
                except KeyError as error:
                    logger.error(
                        f"KeyError: {error} is missing in  found in the input link "
                        "region json file"
                    )
                    exit(1)
            except json.decoder.JSONDecodeError as error:
                logger.error(
                    f"{error} when opening the input link region json file. it "
                    "need to be created first with the CLI push-volumetric"
                )
                exit(1)
                # region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                # link_summary_content.update(region_summary)
            except FileNotFoundError as error:
                logger.error(
                    f"{error} when opening the input link region json file. it "
                    "need to be created first with the CLI push-volumetric"
                )
                exit(1)
                # region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                # link_summary_content.update(region_summary)

        # Add the generation prop for every different atlasRelease
        if differentAtlasrelease and not atlasrelease_payloads["aibs_atlasrelease"]:
            if generation:
                atlasrelease_payloads["hierarchy"].generation = generation
                atlasrelease_payloads["atlas_release"][
                    atlasrelease_choice
                ].generation = generation
            resources_payloads["datasets_toUpdate"][
                f"{const.schema_atlasrelease}"
            ].append(atlasrelease_payloads["atlas_release"][atlasrelease_choice])

        if pushOrUpdate:
            resources_payloads[f"datasets_{action}"][f"{const.schema_mesh}"].append(mesh_resource)

        # =================== Construct the rest of the resources ===================

        for f in range(1, len(files_mesh)):  # start at the 2nd file
            action = "toPush"
            pushOrUpdate = True
            fetched_resource_id = None
            fetched_resource_metadata = None
            meshpath = os.path.join(directory, files_mesh[f])
            try:
                region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
            except ValueError as error:
                logger.error(
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
                logger.error(f"KeyError: {e}")
                exit(1)

            logger.info(f"Creating the Mesh payload for region {region_id}...")
            brainLocation = {
                "brainRegion": {"@id": f"mba:{region_id}", "label": region_name},
                "atlasSpatialReferenceSystem": {
                    "@type": [
                        "BrainAtlasSpatialReferenceSystem",
                        "AtlasSpatialReferenceSystem",
                    ],
                    "@id": const.atlas_spatial_reference_system_id,
                },
            }
            mesh_description = (
                f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for "
                f"the {annotation_description}."
            )

            region_id_str = str(region_id)
            if fetched_resources:
                if region_id_str in fetched_resources:
                    fetched_resource = fetched_resources[region_id_str]
                    fetched_resource_id = fetched_resource.id
                    fetched_resource_metadata = fetched_resource._store_metadata
                    action = "toUpdate"
                    filepath_hash = return_file_hash(meshpath)
                    try:
                        if (
                            filepath_hash
                            != fetched_resource.distribution.digest.value
                        ):
                            content_type = f"application/{file_extension}"
                            distribution_file = forge.attach(meshpath, content_type)
                        else:
                            pushOrUpdate = False
                            distribution_file = fetched_resource.distribution
                    except AttributeError:
                        content_type = f"application/{file_extension}"
                        distribution_file = forge.attach(meshpath, content_type)
                else:
                    content_type = f"application/{file_extension}"
                    distribution_file = forge.attach(meshpath, content_type)
            else:
                content_type = f"application/{file_extension}"
                distribution_file = forge.attach(meshpath, content_type)

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
            if fetched_resource_id:
                mesh_resources.id = fetched_resource_id
            if fetched_resource_metadata:
                mesh_resources._store_metadata = fetched_resource_metadata

            # Finish to fill the link_region_path file with the meshes @ids
            if link_summary_content:
                if hasattr(mesh_resources, "id"):
                    mesh_id = mesh_resources.id
                else:
                    mesh_id = forge.format(
                        "identifier", "brainparcellationmesh", str(uuid4())
                    )
                    mesh_resources.id = mesh_id
                mesh_link = {"mesh": {"@id": mesh_id}}
                # if new_summary_file:
                try:
                    mask_id = link_summary_content[f"{region_id}"]["mask"]["@id"]
                except KeyError as error:
                    logger.error(
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
                    if "mesh" not in link_summary_content[f"{region_id}"].keys():
                        link_summary_content[f"{region_id}"].update(mesh_link)
                    else:
                        link_summary_content[f"{region_id}"]["mesh"] = mesh_link["mesh"]
                except KeyError as error:
                    logger.error(
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

            if pushOrUpdate:
                resources_payloads[f"datasets_{action}"][f"{const.schema_mesh}"].append(mesh_resources)


    pushOrUpdate = False
    for action in actions:
        dataset_action = "datasets_"+action
        resources_payload = resources_payloads[dataset_action][f"{const.schema_mesh}"]
        print(f"Number of resources with schema '{const.schema_mesh}' {action}:", len(resources_payload))
        if resources_payload:
            pushOrUpdate = True

    if not pushOrUpdate:
        print(f"\nNone of the input files (with schema {const.schema_mesh}) differs from its corresponsing version in Nexus")
        return None

    resources_payloads["activity"] = activity_resource
    resources_payloads["tag"] = atlasrelease_payloads["tag"]

    # Annotate the atlasrelease_config json file with the atlasrelease "id" and "tag"
    # TODO Turn it into a function annotate_atlasrelease_file
    if (
        not isinstance(forge._store, DemoStore)
        and not atlasrelease_payloads["aibs_atlasrelease"]
    ):
        if atlasrelease_config_path:
            atlasrelease_id = atlasrelease_payloads["atlas_release"][
                atlasrelease_choice
            ].id
            atlasrelease_link = {
                f"{atlasrelease_choice}": {
                    "id": atlasrelease_id,
                    "tag": atlasrelease_payloads["tag"],
                }
            }
            try:
                with open(atlasrelease_config_path) as atlasrelease_config_file:
                    atlasrelease_config_content = json.loads(
                        atlasrelease_config_file.read()
                    )
                    if atlasrelease_choice in atlasrelease_config_content.keys():
                        atlasrelease_config_content[
                            f"{atlasrelease_choice}"
                        ] = atlasrelease_link[f"{atlasrelease_choice}"]
                    else:
                        atlasrelease_config_content.update(atlasrelease_link)
                with open(atlasrelease_config_path, "w") as atlasrelease_config_file:
                    atlasrelease_config_file.write(
                        json.dumps(
                            atlasrelease_config_content, ensure_ascii=False, indent=2
                        )
                    )
            except json.decoder.JSONDecodeError as error:
                logger.error(f"{error} when opening the input atlasrelease json file.")
                exit(1)

    return resources_payloads
