"""
Create a 'Mesh' resource payload to push into Nexus. This script has been designed to 
function with brain region meshes generated by the Atlas pipeline.
To know more about 'Mesh' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: 
    https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import yaml
import fnmatch
from datetime import datetime
from uuid import uuid4
from kgforge.core import Resource
from kgforge.specializations.stores.demo_store import DemoStore
from bba_data_push.commons import (
    get_brain_region_name,
    get_hierarchy_file,
    add_contribution,
    append_provenance_to_description,
)
from bba_data_push.logging import create_log_handler

L = create_log_handler(__name__, "./push_meshes.log")


def create_mesh_resources(
    forge,
    inputpath: list,
    config_path,
    input_hierarchy: list,
    provenances: list,
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
    module_prov = "parcellation2mesh"
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
    atlasreleases = []
    atlasrelease_dict = {}
    atlasRelease = {}
    for filepath in inputpath:
        flat_tree = {}
        if os.path.isdir(filepath):
            directory = filepath
            files = os.listdir(directory)
            pattern = "*.obj"
            files_mesh = fnmatch.filter(files, pattern)
            if not files_mesh:
                L.error(f"Error: '{filepath}' do not contain any .obj mesh files")
                exit(1)
            isMeshSplit = False
            if not isinstance(forge._store, DemoStore):
                try:
                    atlasrelease_dict = return_atlasrelease(
                        forge,
                        mesh_path,
                        filepath,
                        atlas_reference_system_id,
                        subject,
                    )
                except FileNotFoundError as e:
                    L.error(f"FileNotFoundError: {e}")
                    exit(1)
            try:
                if os.path.samefile(filepath, mesh_path["brain_region_meshes_hybrid"]):
                    hierarchy_tag = "hierarchy"
                    if atlasrelease_dict:
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
                        if atlasrelease_dict["create_ccfv2v3"]:
                            atlasrelease_dict["atlas_release"][
                                0
                            ].contribution = contribution
                            atlasrelease_dict["atlas_release"][
                                1
                            ].contribution = contribution
                            atlasreleases.append(atlasrelease_dict["atlas_release"][0])
                            atlasreleases.append(atlasrelease_dict["atlas_release"][1])
                elif os.path.samefile(
                    filepath, mesh_path["brain_region_meshes_l23split"]
                ):
                    hierarchy_tag = "hierarchy_l23split"
                    if atlasrelease_dict:
                        atlasRelease = {
                            "@id": atlasrelease_dict["atlas_release"].id,
                            "@type": ["AtlasRelease", "BrainAtlasRelease"],
                        }
                        atlasrelease_dict["atlas_release"].contribution = contribution
                        atlasreleases.append(atlasrelease_dict["atlas_release"])
                else:
                    L.error(
                        f"Error: The '{filepath}' folder do not correspond to one of "
                        "the brain meshes folder dataset defined in the MeshFile "
                        "Section of the 'generated dataset' configuration file"
                    )
                    exit(1)
            except FileNotFoundError as error:
                L.error(f"FileNotFoundError: {error}")
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
                f"ValueError in {'meshpath'} file name. {error}. The mesh file names "
                "have to be integer representing their region"
            )
            exit(1)
        extension = os.path.splitext(os.path.basename(meshpath))[1][1:]

        try:
            hierarchy_path = get_hierarchy_file(
                input_hierarchy, config_content, hierarchy_tag
            )
            region_name, hierarchy_tree = get_brain_region_name(
                region_id, hierarchy_path, flat_tree
            )
        except KeyError as e:
            L.error(f"KeyError: {e}")
            exit(1)
        except ValueError as e:
            L.error(f"ValueError: {e}")
            exit(1)

        flat_tree = hierarchy_tree
        L.info(f"Pushing region {region_id}...")

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
        if isMeshSplit:
            mesh_description = (
                f"Brain region mesh - {region_name.title()} (ID: {region_id}). "
                "It is based in the parcellation volume resulting of the hybridation "
                "between CCFv2 and CCFv3 with the isocortex layer 2 and 3 split."
            )
        else:
            mesh_description = (
                f"Brain region mesh - {region_name.title()} (ID: {region_id}). "
                "It is based in the parcellation volume resulting of the hybridation "
                "between CCFv2 and CCFv3."
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

        mesh_resource = Resource(
            type=["BrainParcellationMesh", "Mesh", "Dataset"],
            name=f"{region_name.title()} Mesh",
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
                region_name, hierarchy_tree = get_brain_region_name(
                    region_id, hierarchy_path, flat_tree
                )
            except KeyError as e:
                L.error(f"KeyError: {e}")
                exit(1)
            L.info(f"Pushing region {region_id}...")
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
            if isMeshSplit:
                mesh_description = (
                    f"Brain region mesh - {region_name.title()} "
                    f"(ID: {region_id}). It is based in the parcellation volume "
                    "resulting of the hybridation between CCFv2 and CCFv3 and "
                    "integrating the splitting of layer 2 and layer 3."
                )
            else:
                mesh_description = (
                    f"Brain region mesh - {region_name.title()} "
                    f"(ID: {region_id}). It is based in the parcellation volume "
                    "resulting of the hybridation between CCFv2 and CCFv3."
                )
            if provenances[0]:
                mesh_description = f"{mesh_description} {prov_description}"

            mesh_resources = Resource(
                type=mesh_resource.type,
                name=f"{region_name.title()} Mesh",
                description=mesh_description,
                atlasRelease=mesh_resource.atlasRelease,
                isRegisteredIn=mesh_resource.isRegisteredIn,
                brainLocation=brainLocation,
                spatialUnit=mesh_resource.spatialUnit,
                distribution=distribution_file,
                contribution=mesh_resource.contribution,
                subject=mesh_resource.subject,
            )
            # dataset = Dataset.from_resource(forge, mesh_resources,
            # store_metadata=True)
            datasets.append(mesh_resources)

    return datasets, atlasreleases


def return_atlasrelease(
    forge,
    mesh_path,
    dataset,
    atlas_reference_system_id,
    subject,
):

    spatialReferenceSystem = {
        "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
        "allen_ccfv3_spatial_reference_system",
        "@type": "AtlasSpatialReferenceSystem",
    }

    # average brain model ccfv3
    brainTemplateDataLayer = {
        "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
        "dca40f99-b494-4d2c-9a2f-c407180138b7",
        "@type": "BrainTemplateDataLayer",
    }

    releaseDate = {
        "@type": "xsd:date",
        "@value": f"{datetime.today().strftime('%Y-%m-%d')}",
    }

    if os.path.samefile(dataset, mesh_path["brain_region_meshes_l23split"]):
        # Atlas release hybrid v2-v3 L2L3 split
        try:
            filters = {"name": "Allen Mouse CCF v2-v3 hybrid l2-l3 split"}
            atlasrelease_resource = forge.search(filters, limit=1)[0]
            atlasrelease_dict = {"atlas_release": atlasrelease_resource}
            if not atlasrelease_resource:
                L.error(
                    "No BrainAtlasRelease 'Allen Mouse CCF v2-v3 hybrid l2-l3 "
                    "split' resource found in the destination project "
                    f"'{forge._store.bucket}'. Please use first the CLI "
                    "push-volumetric with the argument "
                    "--new-atlasrelease-hierarchy-path provided to generate and push a "
                    "new atlas release resource into your project ."
                )
                exit(1)
        except Exception as e:
            L.error(
                "Error when searching the BrainAtlasRelease Resource 'Allen "
                "Mouse CCF v2-v3 hybrid l2-l3 split' in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
            exit(1)

    # Old Atlas Releases ccfv2 and ccfv3
    elif os.path.samefile(dataset, mesh_path["brain_region_meshes_hybrid"]):
        try:
            filters = {"name": "Allen Mouse CCF v2"}
            atlasreleasev2_resource = forge.search(filters, limit=1)[0]
            filters = {"name": "Allen Mouse CCF v3"}
            atlasreleasev3_resource = forge.search(filters, limit=1)[0]
            atlasrelease_dict = {
                "atlas_release": [atlasreleasev2_resource, atlasreleasev3_resource],
                "create_ccfv2v3": False,
            }
        except Exception as e:
            L.error(
                "Error when searching the BrainAtlasRelease Resources 'Allen "
                "Mouse CCF v2' and 'Allen Mouse CCF v3'in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
            exit(1)
        if not atlasreleasev2_resource or not atlasreleasev3_resource:
            L.info(
                "No BrainAtlasRelease 'Allen Mouse CCF v2' and 'Allen "
                "Mouse CCF v3' resources found in the destination project "
                f"'{forge._store.bucket}'. They will therefore be created."
            )
            description_ccfv2 = (
                "This atlas release uses the brain parcellation of CCFv2 (2011). The "
                "average brain template and the ontology is common across CCFv2 and "
                "CCFv3."
            )
            name_ccfv2 = "Allen Mouse CCF v2"
            parcellationOntology = {
                "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
                "@type": ["Ontology", "ParcellationOntology"],
            }
            parcellationVolume = {
                "@id": "https://bbp.epfl.ch/neurosciencegraph/data/ "
                "7b4b36ad-911c-4758-8686-2bf7943e10fb",
                "@type": "BrainParcellationDataLayer",
            }

            atlasreleasev2_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name=name_ccfv2,
                description=description_ccfv2,
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                releaseDate=releaseDate,
                subject=subject,
                parcellationOntology=parcellationOntology,
                parcellationVolume=parcellationVolume,
            )

            atlasreleasev3_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name=name_ccfv2.replace("v2", "v3"),
                description=description_ccfv2.replace("CCFv2 (2011)", "CCFv3 (2017)"),
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                releaseDate=releaseDate,
                subject=subject,
                parcellationOntology=parcellationOntology,
                parcellationVolume=parcellationVolume,
            )
            atlasrelease_dict = {
                "atlas_release": [atlasreleasev2_resource, atlasreleasev3_resource],
                "create_ccfv2v3": True,
            }

    return atlasrelease_dict
