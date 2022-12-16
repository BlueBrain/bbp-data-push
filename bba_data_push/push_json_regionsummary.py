"""
Create a 'RegionSummary', an 'atlasRelease' and an 'ontology' resource payload
to push into Nexus. If the resources already exist in Nexus, they will be fetched and
updated instead.
This script has been designed to function with metadata json files generated by the
Atlas pipeline.
To know more about 'RegionSummary' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation:
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import yaml
import json
from kgforge.core import Resource
from kgforge.specializations.stores.demo_store import DemoStore
from bba_data_push.commons import (
    get_brain_region_prop,
    get_hierarchy_file,
    return_contribution,
    return_activity_payload,
    return_atlasrelease,
    fetch_linked_resources,
    return_file_hash,
)
import bba_data_push.constants as const
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_json_regionsummary.log")


def create_regionsummary_resources(
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
    Construct the input RegionSummary dataset, atlasrelease and hierarchy payloads that
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

    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        metadata = config_content["GeneratedDatasetPath"]["MetadataFile"]
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

    # Dict containing all the pipeline generated metadata-json datasets and their
    # informations
    try:
        metadata_dict = const.return_metadata_dict(metadata)
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
            f"{const.schema_regionsummary}": [],
            f"{const.schema_atlasrelease}": [],
            f"{const.schema_ontology}": [],
        },
        "datasets_toPush": {
            f"{const.schema_regionsummary}": [],
            f"{const.schema_atlasrelease}": [],
            f"{const.schema_ontology}": [],
        },
        "activity": [],
        "tag": "",
    }
    atlasrelease_payloads = {
        "atlasrelease_choice": None,
        "atlas_release": {},
        "hierarchy": None,
        "tag": None,
        "fetched": False,
        "aibs_atlasrelease": False,
    }
    atlasRelease = {}
    generation = {}
    activity_resource = []
    for filepath in inputpath:
        fileFound = False
        flat_tree = {}
        fetched_resources = None
        for dataset in metadata_dict:
            try:
                if os.path.samefile(filepath, dataset):
                    fileFound = True
                    metadata_input = open(filepath, "r")
                    metadata_content = json.loads(metadata_input.read())
                    summary_type = metadata_dict[dataset]["type"]
                    hierarchy_tag = metadata_dict[dataset]["hierarchy_tag"]
                    annotation_name = metadata_dict[dataset]["annotation_name"]
                    atlasrelease_choice = metadata_dict[dataset]["atlasrelease"]
                    description_atlas = metadata_dict[dataset]["description"]
            except json.decoder.JSONDecodeError as error:
                L.error(f"JSONDecodeError for '{filepath}' file:  {error}")
                exit(1)
            except FileNotFoundError:
                pass

        # If still no file found at this step then raise error
        if not fileFound:
            L.error(
                f"FileNotFoundError: '{filepath}' file do not correspond to one of "
                "the Metadata json dataset defined in the MetadataFile Section of the "
                "'generated dataset' configuration file"
            )
            exit(1)

        try:
            link_regions_input = open(link_regions_path, "r")
            link_summary_content = json.loads(link_regions_input.read())
        except json.decoder.JSONDecodeError as error:
            L.error(f"JSONDecodeError for '{link_regions_input}' file: {error}")
            exit(1)

        for region_id in metadata_content:
            toUpdate = False
            fetched_resource_id = None
            fetched_resource_metadata = None
            differentAtlasrelease = False
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

            try:
                region_infos, hierarchy_tree = get_brain_region_prop(
                    region_id,
                    ["name", "acronym", "color_hex_triplet"],
                    hierarchy_path,
                    flat_tree,
                )
                if not flat_tree:
                    flat_tree = hierarchy_tree
            except KeyError as e:
                L.error(f"KeyError: {e}")
                exit(1)
            except ValueError as e:
                L.error(f"ValueError: {e}")
                exit(1)

            # ====== Fetch the atlasRelease Resource linked to the input datasets ======

            if not isinstance(forge._store, DemoStore) and not isinstance(
                atlasrelease_choice, dict
            ):
                # Check that the same atlasrelease is not treated again (need to be
                # different + not been treated yet)
                if (
                    atlasrelease_choice
                    not in atlasrelease_payloads["atlas_release"].keys()
                ):
                    differentAtlasrelease = True
                    atlasrelease_payloads["atlasrelease_choice"] = atlasrelease_choice
                    try:
                        atlasrelease_payloads = return_atlasrelease(
                            forge,
                            atlasrelease_config_path,
                            atlasrelease_payloads,
                            resource_tag,
                            isSecondaryCLI=True,
                        )
                        if not atlasrelease_payloads["aibs_atlasrelease"]:
                            if atlasrelease_payloads["fetched"]:
                                L.info(
                                    f"atlasrelease Resource '{atlasrelease_choice}' "
                                    "found in the Nexus destination project "
                                    f"'{forge._store.bucket}'"
                                )
                            else:
                                L.error(
                                    f"atlasrelease Resource '{atlasrelease_choice}' "
                                    "has not been found in the Nexus destination "
                                    f"project '{forge._store.bucket}'. A new one need "
                                    "to be created and pushed into Nexus using the CLI "
                                    "push-volumetric."
                                )
                                exit(1)
                    except Exception as e:
                        L.error(f"Exception: {e}")
                        exit(1)
                    except AttributeError as e:
                        L.error(f"AttributeError: {e}")
                        exit(1)
                else:
                    differentAtlasrelease = False

                if atlasrelease_payloads["aibs_atlasrelease"]:
                    atlasRelease = {
                        "@id": atlasrelease_payloads["aibs_atlasrelease"]["@id"],
                        "@type": atlasrelease_payloads["aibs_atlasrelease"]["@type"],
                    }
                else:
                    atlasRelease = {
                        "@id": atlasrelease_payloads["atlas_release"][
                            atlasrelease_choice
                        ].id,
                        "@type": atlasrelease_payloads["atlas_release"][
                            atlasrelease_choice
                        ].type,
                    }

                    # ======= Check that the atlas Ontology is present in input =======

                    # For a new atlas release creation verify first that the right
                    # parcellation volume and hierarchy file have been provided and
                    # attach the distribution. For an update, compare first if they
                    # distribution are different before attaching it

                    # => check if the good hierarchy file is given in input
                    if (
                        differentAtlasrelease
                        and not atlasrelease_payloads["aibs_atlasrelease"]
                    ):
                        try:
                            atlasrelease_ontology_path = get_hierarchy_file(
                                input_hierarchy,
                                config_content,
                                const.atlasrelease_dict[atlasrelease_choice][
                                    "ontology"
                                ]["name"],
                            )
                        except KeyError:
                            # If the distribution is empty the good file is needed for "
                            # a new creation
                            if not atlasrelease_payloads["hierarchy"].distribution:
                                L.error(
                                    "Error: the ontology file corresponding to the "
                                    "created atlasRelease resource can not be found "
                                    "among input hierarchy files."
                                )
                                exit(1)

                        # Build the distribution dict with the input hierarchy file
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
                        # add it to the distribution dict.
                        try:
                            hierarchy_mba = const.atlasrelease_dict[
                                atlasrelease_choice
                            ]["ontology"]["mba_jsonld"]
                            if os.path.samefile(
                                input_hierarchy_jsonld,
                                hierarchies[hierarchy_mba],
                            ):
                                content_type_mba = "application/ld+json"
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
                        except FileNotFoundError as error:
                            L.error(
                                f"Error : {error}. Input hierarchy jsonLD file "
                                "does not correspond to the input hierarchy "
                                "json file"
                            )
                            exit(1)
                        # if no input hierarchy jsonLD has been provided then
                        # input_hierarchy_jsonld is None and os.path.samefile raises a
                        # TypeError
                        except TypeError:
                            pass

                        # If the hierarchy file has been fetched then the distribution
                        # will be updated with the one given in input only if it is
                        # different from the ones from the distribution dict. For a
                        # brand new file, the distribution will be attached by default.
                        distribution_ontologies = []
                        if atlasrelease_payloads["hierarchy"].distribution:
                            if not isinstance(
                                atlasrelease_payloads["hierarchy"].distribution, list
                            ):
                                atlasrelease_payloads["hierarchy"].distribution = [
                                    atlasrelease_payloads["hierarchy"].distribution
                                ]
                            # Compare the fetched hierarchy file hash with the hash from
                            # the input ones
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
                                        # attach the selected input distribution and
                                        # pop it from the dictionary
                                        distribution_ontologies.append(
                                            distribution_hierarchy
                                        )
                                    else:
                                        # If the distribution is the same, keep it
                                        distribution_ontologies.append(fetched_distrib)
                                    input_hierarchy_distrib.pop(
                                        fetched_distrib.encodingFormat
                                    )
                                # If the distribution is empty
                                except AttributeError:
                                    pass
                                except KeyError:
                                    pass

                            # If still keys in it then attach the remaining files
                            if input_hierarchy_distrib:
                                for encoding, file in input_hierarchy_distrib.items():
                                    distribution_hierarchy = forge.attach(
                                        file[1],
                                        encoding,
                                    )
                                    distribution_ontologies.append(
                                        distribution_hierarchy
                                    )
                        else:
                            # If the hierarchy file is new so it does not have a
                            # distribution then attach the distribution from the input
                            # files
                            for encoding, file in input_hierarchy_distrib.items():
                                distribution_hierarchy = forge.attach(file[1], encoding)
                                distribution_ontologies.append(distribution_hierarchy)

                        atlasrelease_payloads[
                            "hierarchy"
                        ].distribution = distribution_ontologies

                        # ================= Link atlasRelease/Ontology =================

                        if not atlasrelease_payloads["atlas_release"][
                            atlasrelease_choice
                        ].parcellationOntology:
                            atlasrelease_payloads["atlas_release"][
                                atlasrelease_choice
                            ].parcellationOntology = {
                                "@id": atlasrelease_payloads["hierarchy"].id,
                                "@type": ["Entity", const.ontology_type, "Ontology"],
                            }
                        atlasrelease_payloads["hierarchy"].contribution = contribution

                        resources_payloads["datasets_toUpdate"][
                            f"{const.schema_ontology}"
                        ].append(atlasrelease_payloads["hierarchy"])

                # ================ Fetch atlasRelease linked resources ===============
                if differentAtlasrelease:
                    if (
                        atlasrelease_payloads["fetched"]
                        or atlasrelease_payloads["aibs_atlasrelease"]
                    ):
                        resource_type_list = list(
                            set(summary_type).difference(
                                set([const.dataset_type, const.regionsummary_type])
                            )
                        )
                        try:
                            # fetched_resources will be either one resource or a
                            # dictionnary of resource
                            fetched_resources = fetch_linked_resources(
                                forge,
                                atlasRelease,
                                resource_type_list,
                                [],
                                "isRegionSummary",
                                parcellationAtlas_id=None,
                            )
                        except KeyError as error:
                            L.error(f"{error}")
                            exit(1)
                        except IndexError as error:
                            L.error(f"{error}")
                            exit(1)

            # =================== add Activity and generation prop ===================

            if provenance_metadata and not activity_resource:
                try:
                    activity_resource = return_activity_payload(
                        forge, provenance_metadata
                    )
                    if not activity_resource._store_metadata:
                        L.info(
                            "Existing activity resource not found in the Nexus "
                            f"destination project '{forge._store.bucket}'. A new "
                            "activity will be created and registered"
                        )
                except Exception as e:
                    L.error(f"{e}")
                    exit(1)

                generation = {
                    "@type": "Generation",
                    "activity": {
                        "@id": activity_resource.id,
                        "@type": activity_resource.type,
                    },
                }

            # ============================ Payload creation ============================

            region_name = region_infos["name"]
            acronym = region_infos["acronym"]
            color = region_infos["color_hex_triplet"]
            try:
                volume = {
                    "total": {
                        "size": metadata_content[region_id]["regionVolume"],
                        "unitCode": "cubic micrometer",
                    },
                    "ratio": metadata_content[region_id][
                        "regionVolumeRatioToWholeBrain"
                    ],
                }
                adjacentTo = list(
                    map(
                        lambda region: {
                            "@id": f"mba:{region}",
                            "ratio": metadata_content[region_id]["adjacentTo"][region],
                        },
                        metadata_content[region_id]["adjacentTo"],
                    )
                )
                continuousWith = list(
                    map(
                        lambda region: {"@id": f"mba:{region}"},
                        metadata_content[region_id]["continuousWith"],
                    )
                )
            except KeyError as error:
                L.error(f"KeyError: {error} not found in the region metadata file")
                exit(1)
            try:
                mesh = {
                    "@id": link_summary_content[region_id]["mesh"]["@id"],
                    "@type": ["BrainParcellationMesh", "Mesh", "Dataset"],
                }

                mask = {
                    "@id": link_summary_content[region_id]["mask"]["@id"],
                    "@type": [
                        "BrainParcellationMask",
                        "Volumetricdatalayer",
                        "Dataset",
                    ],
                }
            except KeyError as error:
                L.error(f"KeyError: {error} not found in the link_region_path file")
                exit(1)

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
            layers = metadata_content[region_id]["layers"]
            if layers:
                for layer_nbr, layer_id in const.isocortex_layers.items():
                    if layer_nbr in layers:
                        brainLocation["layer"] = {
                            "@id": layer_id,
                            "label": f"layer {layer_nbr}",
                        }

            description = (
                "This is a summary of many informations and metrics about the "
                f"{region_name} as represented in the atlas {description_atlas}"
            )

            L.info(f"Creating the RegionSummary payload for region {region_id}...")

            # If the resource has been fetched, we copy its id and _store_metadata
            if fetched_resources:
                try:
                    fetched_resource = fetched_resources[f"{region_id}"]
                    fetched_resource_id = fetched_resource.id
                    fetched_resource_metadata = fetched_resource._store_metadata
                    toUpdate = True
                except KeyError:
                    pass

            summary_resource = Resource(
                type=summary_type,
                name=f"{region_name.title()} Summary {annotation_name}",
                description=description,
                brainLocation=brainLocation,
                contribution=contribution,
                notation=acronym,  # notation is the property used in the jsonLD
                color=color,
                volume=volume,
                adjacentTo=adjacentTo,
                continuousWith=continuousWith,
                atlasRelease=atlasRelease,
                mesh=mesh,
                mask=mask,
            )

            if generation:
                summary_resource.generation = generation
            if fetched_resource_id:
                summary_resource.id = fetched_resource_id
            if fetched_resource_metadata:
                summary_resource._store_metadata = fetched_resource_metadata

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

            if toUpdate:
                resources_payloads["datasets_toUpdate"][
                    f"{const.schema_regionsummary}"
                ].append(summary_resource)
            else:
                resources_payloads["datasets_toPush"][
                    f"{const.schema_regionsummary}"
                ].append(summary_resource)

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
                L.error(f"{error} when opening the input atlasrelease json file.")
                exit(1)

    return resources_payloads
