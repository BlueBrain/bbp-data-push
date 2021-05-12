This documentation is also available on [Confluence](https://bbpteam.epfl.ch/project/spaces/display/BBKG/bba-data-push).

# Summary:

Description
Source
Arguments for initialize-pusher-cli
Arguments for push_volumetric
Arguments for push_meshes
Arguments for push_cellrecords
Installation

## Description

This module contains command-line executables aka CLIs that take in input atlas pipeline datasets and push them into Nexus along with a resource properties payload, including:

- push_volumetric to create a VolumetricDataLayer resource payload and push it along with the corresponding volumetric input dataset files into Nexus.

- push_meshes to create a Mesh resource payload and push it along with the corresponding brain .OBJ mesh folder input dataset files into Nexus.

- push_cellrecords to create a CellRecordSerie resource payload and push it along with the corresponding Sonata hdf5 file input dataset files into Nexus.


Each CLI can process multiple files/directories at once. 

The input datasets must be one of the datasets listed in the input configuration file given as the argument of the --config option. These datasets has to correspond to the ones generated by the atlas pipeline (these datasets are referenced on the Pipeline Products page.) The configuration yaml file content structure should look like this configuration example :

      HierarchyJson:
          hierarchy: hierarchy.json
          hierarchy_l23split: hierarchy_l23split.json
      GeneratedDatasetPath:
          VolumetricFile:
              annotation_hybrid: annotation_v2v3_hybrid.nrrd
              annotation_l23split: annotation_l23split.nrrd
              cell_densities: cell_densities
              neuron_densities: neuron_densities
          MeshFile:
              brain_region_meshes_hybrid: brain_region_meshes_v2v3_hybrid
              brain_region_meshes_l23split: brain_region_meshes_l23split
          CellRecordsFile:
              cell_records_sonata: cell_records_sonata.h5


The resource property payload include, in addition to various information on the dataset pushed and its content:

- Its provenance : Name and version of the pipeline and the module which generated this dataset.
- The contributor :  References (name, mail...) to the user associated with the input Nexus token (more informations on the Nexus token here) is automatically added in the payload.

These three CLIs are hierarchically grouped in the cli initialize-pusher-cli. This CLI allows the Initialisation of the Forge python framework to communicate with Nexus. The Forge will enable to build and push into Nexus the metadata payload along with the input dataset.
This means that before calling one of the three CLIs, initialize-pusher-cli must first be called. 

Note: the --verbosity argument allows you to print in the console the last resource payload from the list of resource payloads that has been constructed from input datasets that will be pushed into Nexus. If only one dataset has been given as input then its corresponding resource payload will be printed.


## Source
You can find the source of this module here: https://bbpcode.epfl.ch/code/#/admin/projects/dke/blue_brain_atlas_nexus_push


## Arguments for initialize-pusher-cli

Run the dataset pusher CLI starting by the Initialisation of the Forge python framework to communicate with Nexus

### Inputs
--verbose, -v : Verbosity option. If equal True, the last resource payload from the list of resource payloads that has been constructed from input datasets will be printed. (Optional : boolean)
--forge-config-file : Path to the configuration file used to  instantiate the Forge. (Optional, default = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml")
--nexus-env : Nexus environment to use, can be 'dev', staging', 'prod' or the URL of a custom environment. (Optional, default="prod")
--nexus-org : The Nexus organisation to push into. (Optional, default='bbp')
--nexus-org : The Nexus project to push into. (Optional, default='atlas')
--nexus-token-file : Path to the text file containing the Nexus token.


## Arguments for push_volumetric

Construct and push into Nexus as a resource a 'VolumetricDataLayer' property payload along the corresponding files. This module has been designed to function with volumetric annotation or cell density files generated by the Atlas pipeline.

### Inputs
--dataset-path : [multiple paths] The files or directories of file to push on Nexus.
--config : Path to the generated dataset configuration file. This is a yaml file containing the paths to the Atlas pipeline generated dataset.
--provenances : [multiple string] Strings containing the name and version of the module that generated the dataset. They must follow the form '<module_name>:<anything> <version>'.
--voxels-resolution : The Allen annotation volume voxels resolution in microns.


## Arguments for push_meshes

Construct and push into Nexus as a resource a 'Mesh' property payload along the corresponding files. This module has been designed to function with brain region meshes generated by the Atlas pipeline.

### Inputs
--dataset-path : [multiple paths] The files or directories of file to push on Nexus.
--config : Path to the generated dataset configuration file. This is a yaml file containing the paths to the Atlas pipeline generated dataset.
--provenances : [multiple string] Strings containing the name and version of the module that generated the dataset. They must follow the form '<module_name>:<anything> <version>'.
--hierarchy-path : [multiple paths] The path to the json hierarchy file containing an AIBS hierarchy structure.


## Arguments for push_cellrecords

Construct and push into Nexus as a resource a 'CellRecordSeries' property payload along the corresponding files. This module has been designed to function with sonata h5 files storing 3D brain cell positions and orientations generated by the Atlas pipeline.

### Inputs
--dataset-path : [multiple paths] The files or directories of file to push on Nexus.
--config : Path to the generated dataset configuration file. This is a yaml file containing the paths to the Atlas pipeline generated dataset.
--provenances : [multiple string] Strings containing the name and version of the module that generated the dataset. They must follow the form '<module_name>:<anything> <version>'.
--voxels-resolution : The Allen annotation volume voxels resolution in microns.

## Installation
Clone the repository:

git clone https://<YOUR_USERNAME>/code/a/dke/blue_brain_atlas_nexus_push

and install it with pip:

cd blue_brain_atlas_nexus_push

pip install .

The dependencies (nexusforge...) are all available on Pypi and will be automatically installed by Pip.
