"""
Create a 'Mesh' resource payload to push into Nexus. This script has been designed to function
with brain region meshes generated by the Atlas pipeline.
To know more about 'Mesh' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import yaml
import fnmatch
from kgforge.core import Resource 
from bba_dataset_push.commons import (getBrainRegionName, getHierarchyContent, addContribution,
                                      AppendProvenancetoDescription)
from bba_dataset_push.logging import createLogHandler

L = createLogHandler(__name__, "./push_meshes.log")

def createMeshResources(forge, inputpath, config_path, input_hierarchy, provenances):
    """
    Construct the input brain parcellation mesh dataset property payloads that will be push with the c
    orresponding files into Nexus as a resource.
    
    Parameters:
        forge : instantiated and configured forge object.
        inputpath : input datasets paths. These datasets are folder containing mesh .obj files.
        config_path : configuration yaml file path containing the names and paths of the 
                      atlas-pipeline generated datasets.
        input_hierarchy : path to the input hierarchy json file containing input dataset brain 
                          regions hierarchy.
        provenances : string name of the module that generated input datasets.
    
    Returns:
        dataset : list containing as much Resource object as input datasets. Each Resource is 
        defined by an attached input file and its properties described in a payload.
    """
    ## Constructs the payloads schema according to the 2 different possible mesh dataset to be pushed
    config_file = open(config_path)
    config_content = yaml.safe_load(config_file.read().strip())
    config_file.close()
    try:
        mesh_path = config_content["GeneratedDatasetPath"]["MeshFile"]
    except KeyError as error:
        L.error(f'KeyError: {error}. The key ["GeneratedDatasetPath"]["MeshFile"] is not found in the'\
                'push_dataset_config file')
        exit(1)
    # Constructs the Resource properties payloads accordingly to the input atlas Mesh datasets
    for filepath in inputpath:
        flat_tree = {}
        if os.path.isdir(filepath):
            directory = filepath
            files = os.listdir(directory)
            pattern = '*.obj'
            files_mesh = fnmatch.filter(files, pattern)
            if not files_mesh:
                L.error(f"Error: '{filepath}' do not contain any .obj mesh files")
                exit(1)
            isMeshSplit = False
            if filepath == mesh_path["brain_region_meshes_hybrid"]:
                if not len(files_mesh) == 1:
                    L.error(f"Error: The .obj meshes folder '{filepath}' does not contain the right "\
                          f"amount of brain regions mesh .obj files ({len(files_mesh)} instead of "\
                          "1327) corresponding to a brain annotation hybrid")
                    exit(1)
                hierarchy_tag = "hierarchy"
            elif filepath == mesh_path["brain_region_meshes_l23split"]:
                isMeshSplit = True
                if not len(files_mesh) == 1122:
                    L.error(f"Error: The .obj meshes folder '{filepath}' does not contain the right "\
                          f"amount of brain regions mesh .obj files ({len(files_mesh)} instead of "\
                          "1122) corresponding to a brain annotation hybrid with layer 2-3 split")
                    exit(1)
                hierarchy_tag = "hierarchy_l23split"
            else:
                L.error(f"Error: The '{filepath}' folder do not correspond to one of the brain meshes "\
                      "folder dataset defined in the MeshFile section of the 'generated dataset' "\
                      "configuration file")
                exit(1)
        else:
            L.error(f"Error: '{filepath}' is not a directory. The input dataset need to be a directory "\
                   "containing OBJ brain meshes")
            exit(1)
            
        # We create a 1st payload which will serve as template for the others
        meshpath = os.path.join(directory, files_mesh[0])
        region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
        extension = os.path.splitext(os.path.basename(meshpath))[1][1:]
        
        try:
            hierarchy_path = getHierarchyContent(input_hierarchy, config_content, hierarchy_tag)
            region_name, hierarchy_tree = getBrainRegionName(region_id, hierarchy_path, flat_tree)
        except KeyError as e:
            L.error(f"KeyError: {e}")
            exit(1)

        flat_tree = hierarchy_tree
        print(f"Pushing region {region_id}...")
        
        if region_name is None:
            L.error("❌ ", meshpath, " Name not matching a region.")
            exit(1)
        else:
            #Constants
            spatial_unit = 'µm'
            atlas_reference_system_id = 'https://bbp.epfl.ch/neurosciencegraph/data/allen_ccfv3_spatial_reference_system'        
            # We create a 1st payload that will be recycled in case of multiple files to push
            content_type = f"application/{extension}"
            distribution_file = forge.attach(meshpath, content_type)
            if isMeshSplit:
                mesh_description = f"Brain region mesh - {region_name.title()} (ID: {region_id}). "\
                    "It is based in the parcellation volume resulting of the hybridation between CCFv2 "\
                    "and CCFv3 with the isocortex layer 2 and 3 split."
            else:
                mesh_description = f"Brain region mesh - {region_name.title()} (ID: {region_id}). "\
                    "It is based in the parcellation volume resulting of the hybridation between CCFv2 "\
                    "and CCFv3."
            
            if provenances[0]:
                try:
                    prov_description = AppendProvenancetoDescription(provenances, "parcellation2mesh")
                    mesh_description = f"{mesh_description}. {prov_description}"
                except ValueError as e:
                    L.error(f"Value Error in provenance content. {e}")
                    exit(1)
            
            id_atlas_release = 'https://bbp.epfl.ch/neurosciencegraph/data/e2e500ec-fe7e-4888-88b9-b72425315dda'
            # Add the link to the spatial ref system
            isRegisteredIn = {
                "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
                "@id": atlas_reference_system_id
            }
                
            brainLocation = {
                "brainRegion": {
                    "@id": f"mba:{region_id}",
                    "label": region_name
                },
        
                "atlasSpatialReferenceSystem": {
                    "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
                    "@id": atlas_reference_system_id
                }
            }
                
            mesh_resource = Resource(
                type = "BrainParcellationMesh", #["Mesh", ]
                name = f"{region_name.title()} Mesh",
                description= mesh_description,
                atlasRelease = {"@id": id_atlas_release},
                brainLocation = brainLocation,
                distribution = distribution_file,
                isRegisteredIn = isRegisteredIn,
                spatialUnit = spatial_unit
                )

            try:
                mesh_resource.contribution = addContribution(forge, mesh_resource)
            except Exception as e:
                L.error(f"Error: {e}")
                exit(1)
            
            dataset = [mesh_resource]
            
            for f in range(1,len(files_mesh)): #start at the 2nd file
                
                meshpath = os.path.join(directory, files_mesh[f])
                region_id = int(os.path.splitext(os.path.basename(meshpath))[0])
                try:
                    region_name, hierarchy_tree = getBrainRegionName(region_id, hierarchy_path,
                                                                     flat_tree)
                except KeyError as e:
                    L.error(f"KeyError: {e}")
                    exit(1)
                print(f"Pushing region {region_id}...")
                if region_name is None:
                    L.error("❌ ", meshpath, " Name not matching a region.")
                    exit(1)
                else:      
                    distribution_file = forge.attach(meshpath, content_type) 
                    brainLocation = {
                        "brainRegion": {
                            "@id": f"mba:{region_id}",
                            "label": region_name
                        },
                
                        "atlasSpatialReferenceSystem": {
                            "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
                            "@id": atlas_reference_system_id
                        }
                    }
                    if isMeshSplit:
                        mesh_description = f"Brain region mesh - {region_name.title()} "\
                            f"(ID: {region_id}). It is based in the parcellation volume resulting "\
                            "of the hybridation between CCFv2 and CCFv3 and integrating the "\
                            "splitting of layer 2 and layer 3."
                    else:
                        mesh_description = f"Brain region mesh - {region_name.title()} "\
                            f"(ID: {region_id}). It is based in the parcellation volume resulting "\
                            "of the hybridation between CCFv2 and CCFv3."
                    if provenances[0] != 'None':
                        mesh_description = f"{mesh_description} {prov_description}"
                    mesh_resources = Resource(
                        type = mesh_resource.type,
                        name = f"{region_name.title()} Mesh",
                        description= mesh_description,
                        atlasRelease = mesh_resource.atlasRelease,
                        isRegisteredIn = mesh_resource.isRegisteredIn,
                        brainLocation = brainLocation,
                        spatialUnit = mesh_resource.spatialUnit,
                        distribution = distribution_file,
                        contribution = mesh_resource.contribution
                        )
                    dataset.append(mesh_resources)
    
        return dataset
