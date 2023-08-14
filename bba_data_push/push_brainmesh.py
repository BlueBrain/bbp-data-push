"""
Create a 'Mesh' resource payload to push into Nexus
"""

import os
from pathlib import Path

from kgforge.core import Resource
from kgforge.specializations.resources import Dataset

import bba_data_push.commons as comm
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_brainmesh.log")

def create_mesh_resources(input_paths, dataset_type, flat_tree, atlas_release, forge,
    subject, reference_system, contribution, derivation, logger,
) -> list:
    """
    Construct the payload of the Mesh Resources that will be push with the corresponding files into Nexus.

    Parameters
    ----------
    input_paths: list
        input datasets paths. These datasets is either a mesh file or folder containing mesh files
    dataset_type: str
        type of the Resources to build
    flat_tree: dict
        flatten input hierarchy of the brain regions
    atlas_release: Resource
        atlas release info
    forge: KnowledgeGraphForge
        instance of forge
    subject: Resource
        species info
    reference_system: Resource
        reference system info
    contribution: list
        contributor Resources
    derivation: Resource
        derivation Resource
    logger: Logger
        log_handler

    Returns
    -------
    resources: list
        Resources to be pushed in Nexus.
    """

    extension = ".obj"

    resources = []

    file_paths = []
    for input_path in input_paths:
        if input_path.endswith(extension):
            if os.path.isfile(input_path):
                file_paths.append(input_path)
        elif os.path.isdir(input_path):
            file_paths.extend([str(path) for path in Path(input_path).rglob("*"+extension)])

    tot_files = len(file_paths)
    logger.info(f"{tot_files} {extension} files found under '{input_paths}', creating the respective payloads...")

    file_count = 0
    for filepath in file_paths:
        file_count += 1

        filename_split = os.path.splitext(os.path.basename(filepath))
        region_id = filename_split[0]

        logger.info(f"Creating Mesh payload for file '{region_id}' ({file_count} of {tot_files})")

        region_prefix = forge.get_model_context().expand("mba")
        mba_region_id = region_prefix + region_id
        region_label = comm.get_region_label(flat_tree, int(region_id))
        brain_region = Resource(
            id=mba_region_id,
            label=region_label)

        name = f"Mesh of {region_label}"
        description = f"Mesh of the region {name}."

        brain_location = comm.get_brain_location_prop(brain_region, reference_system)

        mesh_resource = Dataset(forge,
            type=comm.all_types[dataset_type],
            name=name,
            filepath=filepath,
            distribution=forge.attach(filepath, f"application/{extension[1:]}"),
            description=description,
            isRegisteredIn=reference_system,
            brainLocation=brain_location,
            atlasRelease=atlas_release,
            subject=subject,
            spatialUnit="µm",
            contribution=contribution,
            derivation=[derivation]
        )

        logger.info("Payload creation completed\n")

        resources.append(mesh_resource)

    return resources
