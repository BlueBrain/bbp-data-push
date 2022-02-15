"""
Create a 'RegionSummary' resource payload to push into Nexus. This script has been 
designed to function with metadata json files generated by the Atlas pipeline.
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
)
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_json_regionsummary.log")


def create_regionsummary_resources(
    forge,
    inputpath: list,
    config_path,
    input_hierarchy: list,
    link_regions_path,
    provenance_metadata_path,
    verbose,
) -> list:
    """
    Construct the input RegionSummary dataset property payload that will be push
    into Nexus as a resource.

    Parameters:
        forge : instantiated and configured forge object.
        inputpath : input datasets paths. These datasets are folder containing mesh
                    .obj files.
        config_path : configuration yaml file path containing the names and paths of
                      the atlas-pipeline generated datasets.
        input_hierarchy : path to the input hierarchy json file containing input
                          dataset brain regions hierarchy.

    Returns:
        datasets : list containing as much Resource object as input datasets. Each
                   Resource is defined by an attached input file and its properties
                   described in a payload.
    """
    L.setLevel(verbose)

    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        metadata_path = config_content["GeneratedDatasetPath"]["MetadataFile"]
    except KeyError as error:
        L.error(
            f"KeyError: {error}. The key ['GeneratedDatasetPath']['MetadataFile'] is "
            "not found in the dataset configuration file"
        )
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

    # Constants
    ressources_dict = {
        "datasets": [],
        "activity": [],
        "atlasreleases": [],
        "hierarchy": [],
    }
    atlasRelease = {}
    generation = {}
    activity_resource = []
    # module_prov = "parcellationexport"
    atlas_reference_system_id = (
        "https://bbp.epfl.ch/neurosciencegraph/data/"
        "allen_ccfv3_spatial_reference_system"
    )
    annotation_name = "Ccfv3 L23split"
    hierarchy_tag = "hierarchy_l23split"

    description_ccfv3 = "original Allen ccfv3"
    description_split = "with the isocortex layer 2 and 3 split"
    description_ccfv3_split = f"{description_ccfv3} {description_split}"

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

    try:
        if os.path.samefile(
            inputpath[0], metadata_path["metadata_parcellations_ccfv3_l23split"]
        ):
            metadata_input = open(inputpath[0], "r")
            metadata_content = json.loads(metadata_input.read())
        else:
            L.error(
                f"FileNotFoundError: '{inputpath[0]}' file do not correspond to one of "
                "the Metadata json dataset defined in the MetadataFile Section of the "
                "'generated dataset' configuration file"
            )
            exit(1)
    except json.decoder.JSONDecodeError as error:
        L.error(f"JSONDecodeError for '{inputpath[0]}' file:  {error}")
        exit(1)
    except FileNotFoundError as error:
        L.error(f"FileNotFoundError: {error}")
        exit(1)

    try:
        link_regions_input = open(link_regions_path, "r")
        link_summary_content = json.loads(link_regions_input.read())
    except json.decoder.JSONDecodeError as error:
        L.error(f"JSONDecodeError for '{link_regions_input}' file: {error}")
        exit(1)

    hierarchy_path = get_hierarchy_file(input_hierarchy, config_content, hierarchy_tag)

    flat_tree = {}
    for region_id in metadata_content:
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

        region_name = region_infos["name"]
        acronym = region_infos["acronym"]
        color = region_infos["color_hex_triplet"]
        try:
            volume = {
                "total": {
                    "size": metadata_content[region_id]["regionVolume"],
                    "unitCode": "cubic micrometer",
                },
                "ratio": metadata_content[region_id]["regionVolumeRatioToWholeBrain"],
            }
            layers = metadata_content[region_id]["layers"]
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
            atlasRelease = {
                "@id": link_summary_content[region_id]["atlasRelease"]["@id"],
                "@type": ["AtlasRelease", "BrainAtlasRelease", "Entity"],
            }

            mesh = {
                "@id": link_summary_content[region_id]["mesh"]["@id"],
                "@type": ["BrainParcellationMesh", "Mesh", "Dataset"],
            }

            mask = {
                "@id": link_summary_content[region_id]["mask"]["@id"],
                "@type": ["BrainParcellationMask", "Volumetricdatalayer", "Dataset"],
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
                "@id": atlas_reference_system_id,
            },
        }

        description = (
            "This is a summary of many informations and metrics about the "
            f"{region_name} as represented in the atlas {description_ccfv3_split}"
        )

        L.info(f"Creating the RegionSummary payload for region {region_id}...")

        if provenance_metadata:
            try:
                activity_resource = return_activity_payload(forge, provenance_metadata)
            except Exception as e:
                L.error(f"Error: {e}")
                exit(1)

            # # if activity has been created and not fetched from Nexus
            # if activity_resource._store_metadata:
            #     if hasattr(activity_resource, "startedAtTime"):
            #         activity_resource.startedAtTime = forge.from_json(
            #             {
            #                 "type": activity_resource.startedAtTime.type,
            #                 "@value": activity_resource.startedAtTime.value,
            #             }
            #         )

            generation = {
                "@type": "Generation",
                "activity": {
                    "@id": activity_resource.id,
                    "@type": activity_resource.type,
                },
            }

        summary_resource = Resource(
            type=["RegionSummary", "Entity"],
            name=f"{region_name.title()} Summary {annotation_name}",
            description=description,
            brainLocation=brainLocation,
            contribution=contribution,
            acronym=acronym,
            color=color,
            volume=volume,
            layers=layers,
            adjacentTo=adjacentTo,
            continuousWith=continuousWith,
            atlasRelease=atlasRelease,
            mesh=mesh,
            mask=mask,
        )

        if generation:
            summary_resource.generation = generation

        ressources_dict["datasets"].append(summary_resource)

    ressources_dict["activity"] = activity_resource

    return ressources_dict
