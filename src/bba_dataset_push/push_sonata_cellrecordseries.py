"""
Create a 'CellRecordSeries' resource payload to push into Nexus. This script has been designed to function
with sonata hdf5 files storing 3D brain cell positions and orientations and generated by the Atlas pipeline.
To know more about 'Mesh' resources and Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import yaml
import h5py 
from kgforge.core import Resource 
from bba_dataset_push.commons import addContribution, AppendProvenancetoDescription
from bba_dataset_push.logging import createLogHandler

L = createLogHandler(__name__, "./push_cellrecord.log")

def createCellRecordResources(forge, inputpath, voxels_resolution, config_path, provenances):
    """
    Construct the input sonata hdf5 dataset property payloads that will be push with the c
    orresponding files into Nexus as a resource.
    
    Parameters:
        forge : instantiated and configured forge object.
        inputpath : input datasets paths. These datasets are either volumetric files or folder 
                    containing volumetric files.
        voxels_resolution : voxel resolution value.
        config_path : configuration yaml file path containing the names and paths of the 
                      atlas-pipeline generated datasets.
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
        sonata_path = config_content["GeneratedDatasetPath"]["CellPositionFile"]
    except KeyError as error:
        L.error(f'KeyError: {error}. The key ["GeneratedDatasetPath"]["CellPositionFile"] is "\
                "not found in the push_dataset_config file')
        exit(1)
    # Constructs the Resource properties payloads accordingly to the input atlas Mesh datasets
    dataset = []
    Measures_table = {
        "x": "Cell position along the X axis",
        "y": "Cell position along the Y axis",
        "z": "Cell position along the Z axis",
        "orientation_w": "Component w of the cell orientation quaternion",
        "orientation_x": "Component x of the cell orientation quaternion",
        "orientation_y": "Component y of the cell orientation quaternion",
        "orientation_z": "Component z of the cell orientation quaternion",
        "cell_type": "Label of the cell type",
        "region_id": "Region identifiers (AIBS Structure IDs)"
        }
    for filepath in inputpath:
        if filepath == sonata_path["cell_positions_hybrid"]:
            atlas_description = "ccfv2-ccfv3 Hybrid annotation volume"
            #atlas_alias = "Hybrid ccfv2v3" #for the name
        elif filepath == sonata_path["cell_positions_l23split"]:
            atlas_description = "ccfv2-ccfv3 Hybrid annotation volume with the isocortex layer "\
                                "2 and 3 split"
            #atlas_alias = "Hybrid ccfv2v3 l23split" #for the name
        else:
            L.error(f"Error: The '{filepath}' folder do not correspond to a Sonata .h5 file "\
                  "dataset defined in the CellPositionFile section of the 'generated dataset' "\
                  "configuration file")
            exit(1)
        
        # We create a 1st payload which will serve as template for the others
        filename_noext = os.path.splitext(os.path.basename(filepath))[0]
        #file_extension = os.path.splitext(os.path.basename(filepath))[1][1:] if needed
        
        #Constants
        spatial_unit = 'µm'
        atlas_reference_system_id = 'https://bbp.epfl.ch/neurosciencegraph/data/allen_ccfv3_spatial_reference_system' 
        id_atlas_release = 'https://bbp.epfl.ch/neurosciencegraph/data/e2e500ec-fe7e-4888-88b9-b72425315dda'
        region_id = 997 #default: 997 --> root, whole brain
        region_name = "root"
        # We create a 1st payload that will be recycled in case of multiple files to push
        
        description = f"Sonata .h5 file storing the 3D cell positions and orientations of the "\
                      f"{atlas_description} (spatial resolution of {voxels_resolution} "\
                      f"{spatial_unit})."
        
        if provenances[0]:
            try:
                prov_description = AppendProvenancetoDescription(provenances, 
                                                                 "positions-and-orientations")
                description = f"{description}. {prov_description}"
            except ValueError as e:
                L.error(f"Value Error in provenance content. {e}")
                exit(1)
        
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
        try:
            cell_collections = h5py.File(filepath, 'r')
        except OSError as e:
            L.error(f"OSError when trying to open the input file {filepath}. {e}")
            L.info("Aborting pushing process.") #setLevel(logging.INFO)
            exit(1)
        
        recordMeasure = []
        try:
            Datasets = cell_collections['nodes']["atlas_cells"]['0']
            for Dataset in Datasets.keys():
                if Dataset in Measures_table:
                    Measure_payload = {
                              "@type": "RecordMeasure",
                              "description": Measures_table[Dataset],
                              "componentEncoding": f"{Datasets[Dataset].dtype}",
                              "name": f"{Dataset}"
                              }
                    if Dataset == "cell_type":
                        Measure_payload["label"] = {
                            f"{i}" : Datasets["@library"]["cell_type"][i] 
                            for i in range(0, len(Datasets["@library"]["cell_type"]))}
                    recordMeasure.append(Measure_payload)
        except KeyError as e:
            L.error(f"KeyError during the information extraction of the dataset in the input "\
                    f"file {filepath}. {e}")
            exit(1)
        
        #content_type = "application/" + extension
        distribution_file = forge.attach(filepath) #content_type
        
        cellrecord_resource = Resource(
            type = "CellRecordSeries",
            name = filename_noext.replace("_", " ").title(),
            description= description,
            atlasRelease = {"@id": id_atlas_release},
            isRegisteredIn = isRegisteredIn,
            brainLocation = brainLocation,
            distribution = distribution_file,
            recordMeasure = recordMeasure
            )
        
        #name = f"Sonata cell positions orientations {atlas_alias} {voxels_resolution} f"{spatial_unit}"
        #resource.fileExtension = config["file_extension"]
        try:
            cellrecord_resource.contribution = addContribution(forge, cellrecord_resource)
        except Exception as e:
            L.error(f"Error: {e}")
            exit(1)
            
        dataset.append(cellrecord_resource)
    
    return dataset