"""
Create a 'Mesh' resource payload to push into Nexus. This script has been designed to 
function with brain region meshes generated by the Atlas pipeline.
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
    add_contribution,
    append_provenance_to_description,
    return_atlasrelease,
)
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_brainmesh.log")


def create_mesh_resources(
    forge,
    inputpath: list,
    config_path,
    input_hierarchy: list,
    voxels_resolution: int,
    provenances: list,
    link_regions_path,
    verbose,
) -> list:
    """
    Construct the input brain mesh dataset property payloads that will be push with the
    corresponding files into Nexus as a resource.

    Parameters:
        forge : instantiated and configured forge object.
        inputpath : input datasets paths. These datasets are folder containing mesh
                    .obj files.
        config_path : configuration yaml file path containing the names and paths of
                      the atlas-pipeline generated datasets.
        input_hierarchy : path to the input hierarchy json file containing input
                          dataset brain regions hierarchy.
        provenances : string name of the module that generated input datasets.

    Returns:
        datasets : list containing as much Resource object as input datasets. Each
                   Resource is defined by an attached input file and its properties
                   described in a payload.
    """
    L.setLevel(verbose)

    # Constructs the payloads schema according to the 2 different possible mesh
    # dataset to be pushed
    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        mesh_path = config_content["GeneratedDatasetPath"]["MeshFile"]
    except KeyError as error:
        L.error(
            f"KeyError: {error}. The key ['GeneratedDatasetPath']['MeshFile'] is not "
            "found in the dataset configuration file"
        )
        exit(1)

    # Constants
    datasets = []
    module_prov = "parcellationexport"
    spatial_unit = "µm"
    atlas_reference_system_id = (
        "https://bbp.epfl.ch/neurosciencegraph/data/"
        "allen_ccfv3_spatial_reference_system"
    )

    # Link to the spatial ref system
    isRegisteredIn = {
        "@type": ["BrainAtlasSpatialReferenceSystem", "AtlasSpatialReferenceSystem"],
        "@id": atlas_reference_system_id,
    }

    subject = {
        "@type": "Subject",
        "species": {
            "@id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
            "label": "Mus musculus",
        },
    }

    description_ccfv3 = (
        f"original Allen ccfv3 annotation volume at {voxels_resolution} {spatial_unit}"
    )
    description_hybrid = (
        f"Hybrid annotation volume from ccfv2 and ccfv3 at {voxels_resolution} "
        f"{spatial_unit}"
    )
    description_split = "with the isocortex layer 2 and 3 split"
    description_ccfv3_split = f"{description_ccfv3} {description_split}"
    description_hybrid_split = f"{description_hybrid} {description_split}"

    # Create contribution
    if isinstance(forge._store, DemoStore):
        contribution = []
    else:
        try:
            contribution, log_info = add_contribution(forge)
            L.info("\n".join(log_info))
        except Exception as e:
            L.error(f"Error: {e}")
            exit(1)

    # Constructs the Resource properties payloads accordingly to the input atlas Mesh
    # datasets
    atlasreleases = {"atlas_releases": [], "hierarchy": []}
    atlasrelease_dict = {"atlasrelease_choice": False, "hierarchy": False}
    atlasRelease = {}
    for filepath in inputpath:
        file_found = False
        flat_tree = {}
        new_summary_file = False
        link_summary_content = {}
        if os.path.isdir(filepath):
            directory = filepath
            files = os.listdir(directory)
            pattern = "*.obj"
            files_mesh = fnmatch.filter(files, pattern)
            if not files_mesh:
                L.error(f"Error: '{filepath}' do not contain any .obj mesh files")
                exit(1)
            try:
                if os.path.samefile(filepath, mesh_path["brain_region_meshes_hybrid"]):
                    file_found = True
                    hierarchy_tag = "hierarchy"
                    annotation_name = "Hybrid"
                    atlasrelease_choice = "atlasrelease_ccfv2v3"
                    annotation_description = description_hybrid
            except FileNotFoundError:
                pass

            if not file_found:
                try:
                    if os.path.samefile(
                        filepath, mesh_path["brain_region_meshes_hybrid_l23split"]
                    ):
                        file_found = True
                        hierarchy_tag = "hierarchy_l23split"
                        annotation_name = "Hybrid L23split"
                        atlasrelease_choice = "atlasrelease_hybridsplit"
                        annotation_description = description_hybrid_split
                except FileNotFoundError:
                    pass

            if not file_found:
                try:
                    if os.path.samefile(
                        filepath, mesh_path["brain_region_meshes_ccfv3_l23split"]
                    ):
                        file_found = True
                        hierarchy_tag = "hierarchy_l23split"
                        annotation_name = "CCFv3 L23split"
                        atlasrelease_choice = "atlasrelease_ccfv3split"
                        annotation_description = description_ccfv3_split
                except FileNotFoundError:
                    pass
            # If still no file found at this step then raise error
            if not file_found:
                L.error(
                    f"Error: The '{filepath}' folder do not correspond to one of "
                    "the brain meshes folder dataset defined in the MeshFile "
                    "Section of the 'generated dataset' configuration file"
                )
                exit(1)
        else:
            L.error(
                f"Error: '{filepath}' is not a directory. The input dataset need to be "
                "a directory containing OBJ brain meshes"
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
        extension = os.path.splitext(os.path.basename(meshpath))[1][1:]

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
        content_type = f"application/{extension}"
        distribution_file = forge.attach(meshpath, content_type)

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
        mesh_description = (
            f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for the "
            f"{annotation_description}."
        )

        if provenances[0]:
            try:
                prov_description = append_provenance_to_description(
                    provenances, module_prov
                )
                mesh_description = f"{mesh_description} {prov_description}"
            except ValueError as e:
                L.error(f"Value Error in provenance content: {e}")
                exit(1)

        if atlasrelease_choice == "atlasrelease_ccfv3split":
            atlasRelease = {
                "@id": "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease/"
                "5149d239-8b4d-43bb-97b7-8841a12d85c4",
                "@type": ["AtlasRelease", "BrainAtlasRelease"],
            }
        elif not isinstance(forge._store, DemoStore):
            if not atlasrelease_dict["atlasrelease_choice"] or (
                atlasrelease_choice != atlasrelease_dict["atlasrelease_choice"]
            ):
                atlasrelease_dict["atlasrelease_choice"] = atlasrelease_choice
                try:
                    atlasrelease_dict = return_atlasrelease(
                        forge=forge,
                        config_content={},
                        new_atlasrelease_hierarchy_path=False,
                        atlasrelease_dict=atlasrelease_dict,
                        parcellation_found=False,
                        atlas_reference_system_id=atlas_reference_system_id,
                        subject=subject,
                    )
                except Exception as e:
                    L.error(f"Exception: {e}")
                    exit(1)
                except FileNotFoundError as e:
                    L.error(f"FileNotFoundError: {e}")
                    exit(1)
            # if atlasrelease are ccfv2 and ccfv3
            if isinstance(atlasrelease_dict["atlas_release"], list):
                atlasRelease = [
                    {
                        "@id": atlasrelease_dict["atlas_release"][0].id,
                        "@type": ["AtlasRelease", "BrainAtlasRelease"],
                    },
                    {
                        "@id": atlasrelease_dict["atlas_release"][1].id,
                        "@type": ["AtlasRelease", "BrainAtlasRelease"],
                    },
                ]
                if atlasrelease_dict["create_new"]:
                    atlasrelease_dict["atlas_release"][0].contribution = contribution
                    atlasrelease_dict["atlas_release"][1].contribution = contribution
                    atlasreleases["atlas_releases"].append(
                        atlasrelease_dict["atlas_release"][0]
                    )
                    atlasreleases["atlas_releases"].append(
                        atlasrelease_dict["atlas_release"][1]
                    )
            else:
                atlasRelease = {
                    "@id": atlasrelease_dict["atlas_release"].id,
                    "@type": ["AtlasRelease", "BrainAtlasRelease"],
                }

        mesh_resource = Resource(
            type=["BrainParcellationMesh", "Mesh", "Dataset"],
            name=f"{region_name.title()} Mesh {annotation_name}",
            description=mesh_description,
            atlasRelease=atlasRelease,
            brainLocation=brainLocation,
            distribution=distribution_file,
            isRegisteredIn=isRegisteredIn,
            spatialUnit=spatial_unit,
            subject=subject,
            contribution=contribution,
        )
        # dataset = Dataset.from_resource(forge, mesh_resource, store_metadata=True)

        if link_regions_path:
            mesh_id = forge.format("identifier", "BrainParcellationMesh", str(uuid4()))
            mesh_resource.id = mesh_id
            mesh_link = {"mesh": {"@id": mesh_id}}
            try:
                with open(link_regions_path, "r+") as link_summary_file:
                    link_summary_file.seek(0)
                    link_summary_content = json.loads(link_summary_file.read())
                try:
                    new_summary_file = True
                    if "mesh" not in link_summary_content[f"{region_id}"].keys():
                        link_summary_content[f"{region_id}"].update(mesh_link)
                except KeyError as error:
                    L.error(
                        f"KeyError: The region whose region id is '{error}' can "
                        "not be found in the input link region json file"
                    )
                    exit(1)
            except json.decoder.JSONDecodeError:
                region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                link_summary_content.update(region_summary)

        datasets = [mesh_resource]

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
                    "@id": atlas_reference_system_id,
                },
            }
            mesh_description = (
                f"Brain region mesh - {region_name.title()} (ID: {region_id}) - for the "
                f"{annotation_description}."
            )
            if provenances[0]:
                mesh_description = f"{mesh_description} {prov_description}"

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

            if link_summary_content:
                mesh_id = forge.format(
                    "identifier", "BrainParcellationMesh", str(uuid4())
                )
                mesh_resources.id = mesh_id
                mesh_link = {"mesh": {"@id": mesh_id}}
                if new_summary_file:
                    try:
                        if "mesh" not in link_summary_content[f"{region_id}"].keys():
                            link_summary_content[f"{region_id}"].update(mesh_link)
                    except KeyError as error:
                        L.error(
                            f"KeyError: The region whose region id is '{error}' "
                            "can not be found in the input link region json file"
                        )
                        exit(1)
                else:
                    region_summary = {f"{region_id}": {"mesh": {"@id": mesh_id}}}
                    link_summary_content.update(region_summary)
                if f == len(files_mesh) - 1:
                    link_summary_file = open(link_regions_path, "w")
                    link_summary_file.write(
                        json.dumps(link_summary_content, ensure_ascii=False, indent=2)
                    )
                    link_summary_file.close()

            # dataset = Dataset.from_resource(forge, mesh_resources,
            # store_metadata=True)
            datasets.append(mesh_resources)

    return datasets, atlasreleases
