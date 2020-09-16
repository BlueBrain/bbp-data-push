"""
Volumetric data
"""

import os
import numpy as np
import nrrd
from kgforge.core import Resource 

from commons import getBrainRegionNameAllen, getVoxelType


def createResource(forge, nrrd_filepath, extra_types, id_vol, spatial_unit):
    
    #TODO
    default_sampling_period = 30
    default_sampling_time_unit = "ms"
        
    resource_types =  'VolumetricDataLayer'
    if extra_types:
        resource_types = [resource_types]
        for extratype in extra_types:
            resource_types.append(extratype)  
    
    # # If the file exists
    # if nrrd_nexus_id is not None:
    #     resource_file = None
    #     dl_folder = "/tmp/"
    #     resource_file = forge.retrieve(id = nrrd_nexus_id) 
    #     forge.download(resource_file, "distribution.contentUrl", dl_folder)
    #     nrrd_filepath = dl_folder + resource_file.name
    #     print("............................... FILE .............................")
    #     print(resource_file) # to display response/error
    #     print("..................................................................")
    #     # TODO: delete tmp file at some point
    
    # this is going ot be the "name" of the resource
    filename_noext = os.path.splitext(os.path.basename(nrrd_filepath))[0]
    file_extension = os.path.splitext(os.path.basename(nrrd_filepath))[1][1:]

    # Parsing the header of the NRRD file
    header = None
    try:
        header = nrrd.read_header(nrrd_filepath)
    except nrrd.errors.NRRDError as e:
        print(e)
        print("Aborting pushing process.")
        exit(1)
    
    distribution_file = forge.attach(nrrd_filepath) #content_type = "application/" + file_extension
    
    config = {
        "file_extension": file_extension,
        "sampling_space_unit": spatial_unit,
        "sampling_period": default_sampling_period,
        "sampling_time_unit": default_sampling_time_unit
    }
        
    nrrd_resource = Resource(type = resource_types, 
                             name = filename_noext, 
                             distribution = distribution_file
                             )

    return nrrd_resource, header, config


def addSpatialRefSystem(dataset, forge, brain_region_id, id_spatial_ref, id_atlas_release, description):
    
    atlas_reference_system_payload = forge.retrieve(id = id_spatial_ref, cross_bucket = True)
    
    # Add the link to the spatial ref system
    isRegisteredIn = {
        "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
        "@id": atlas_reference_system_payload.id  #atlas_spatial_ref_extended_id
    }
    dataset.isRegisteredIn = isRegisteredIn

    brainLocation = {
        # "@type": "BrainLocation",
        "brainRegion": {
            # "@id": "http://api.brain-map.org/api/v2/data/Structure/" + str(brain_region_id),
            "@id": "mba:" + str(brain_region_id),
            "label": getBrainRegionNameAllen(brain_region_id) #getBrainRegionNameNexus(brain_region_id)
        },

        "atlasSpatialReferenceSystem": {
            "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
            "@id": atlas_reference_system_payload.id  #atlas_spatial_ref_extended_id
        }
    }
    dataset.brainLocation = brainLocation

    if description:
        dataset.description = description
    
    dataset.atlasRelease = {"@id": id_atlas_release}
    
    return dataset


def addNrrdProps(resource, nrrd_header, config, voxel_type):
    """
    Add to the resource all the fields expected for a VolumetricDataLayer/NdRaster
    that can be found in the NRRD header.
    A resource dictionary must exist and be provided (even if empty).
    """
    
    NRRD_TYPES_TO_NUMPY = {
    "signed char": "int8",
    "int8": "int8",
    "int8_t": "int8",
    "uchar": "uint8",
    "unsigned char": "uint8",
    "uint8": "uint8",
    "uint8_t": "uint8",
    "short": "int16",
    "short int": "int16",
    "signed short": "int16",
    "signed short int": "int16",
    "int16": "int16",
    "int16_t": "int16",
    "ushort": "int16",
    "unsigned short": "uint16",
    "unsigned short int": "uint16",
    "uint16": "uint16",
    "uint16_t": "uint16",
    "int": "int32",
    "signed int": "int32",
    "int32": "int32",
    "int32_t": "int32",
    "uint": "uint32",
    "unsigned int": "uint32",
    "uint32": "uint32",
    "uint32_t": "uint32",
    "longlong": "int64",
    "long long": "int64",
    "long long int": "int64",
    "signed long long": "int64",
    "signed long long int": "int64",
    "int64": "int64",
    "int64_t": "int64",
    "ulonglong": "uint64",
    "unsigned long long": "uint64",
    "unsigned long long int": "uint64",
    "uint64": "uint64",
    "uint64_t": "uint64",
    "float": "float32",
    "double": "float64"
    }
    
    space_origin = None
    if "space origin" in nrrd_header:
        space_origin = nrrd_header["space origin"].tolist()
    else:
        if nrrd_header["dimension"] == 2:
            space_origin = [0.0, 0.0]
        elif nrrd_header["dimension"] == 3:
            space_origin = [0.0, 0.0, 0.0]

    space_directions = None
    if "space directions" in nrrd_header:
        # replace the nan that pynrrd adds to None (just like in NRRD spec)
        space_directions = []
        for col in nrrd_header["space directions"].tolist():
            if np.isnan(col).any():
                space_directions.append(None)
            else:
                space_directions.append(col)

    # Here, 'space directions' being missing in the file, we hardcode an identity matrix.
    # If we have 4 dimensions, we say
    else:
        if nrrd_header["dimension"] == 2:
            space_directions = [[1, 0], [0, 1]]
        elif nrrd_header["dimension"] == 3:
            space_directions = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
        elif nrrd_header["dimension"] == 4:
            # the following is a very lousy way to determine if among the 4 dims,
            # or the first is components or the last is time...
            if nrrd_header["sizes"][0] < (np.mean(nrrd_header["sizes"] * 0.20)):
                space_directions = [None, [1, 0, 0], [0, 1, 0], [0, 0, 1]] # component
            else:
                space_directions = [[1, 0, 0], [0, 1, 0], [0, 0, 1], None] # time

        elif nrrd_header["dimension"] == 5:
            space_directions = [None, [1, 0, 0], [0, 1, 0], [0, 0, 1], None]

    resource.componentEncoding = NRRD_TYPES_TO_NUMPY[nrrd_header["type"]]
    resource.endianness = nrrd_header["endian"]
    resource.bufferEncoding = nrrd_header["encoding"]
    resource.fileExtension = config["file_extension"]
    resource.dimension = []

    component_dim_index = -1
    passed_spatial_dim = False
    # for each dimension
    for i in range(0, nrrd_header["dimension"]):
        current_dim = {}
        current_dim["size"] = nrrd_header["sizes"][i].item()


        # this is a spatial dim
        if space_directions[i]:
            passed_spatial_dim = True
            current_dim["@type"] = "SpaceDimension"
            current_dim["unitCode"] = config["sampling_space_unit"]

        # this can be a component or a time dim
        else:
            # this is a time dim (because after space dim)
            if passed_spatial_dim:
                current_dim["@type"] = "nsg:TimeDimension"
                current_dim["samplingPeriod"] = config["sampling_period"]
                current_dim["unitCode"] = config["sampling_time_unit"]

            # this is a component dim (because before space dim)
            else:
                # decide of the label

                component_dim_index = i
                current_dim["@type"] = "ComponentDimension"
                # current_dim["name"] = default_sample_type_multiple_components if current_dim["size"] > 1 else default_sample_type_single_component
                current_dim["name"] = getVoxelType(current_dim["size"])

        resource.dimension.append(current_dim)

    # repeating the name of the component dimension in the "sampleType" base level prop
    if component_dim_index >= 0:
        resource.sampleType = resource.dimension.component_dim_index.name

    # As no component dim was mentioned in metadata, it means the component is of size 1
    else:
        # prepend a dimension component
        component_dim = {
            "@type": "ComponentDimension",
            "size": 1,
            "name": getVoxelType(voxel_type, 1)
        }
        resource.dimension.insert(0, component_dim)

        resource.sampleType = component_dim["name"]

    # creating the world matrix (column major)
    # 1. pynrrd creates a [nan, nan, nan] line for each 'space directions' that is 'none' in the header.
    # We have to strip them off.
    worldMatrix = None
    r = [] # rotation mat
    o = space_origin
    for col in space_directions:
        if col != None:
            r.append(col)

    # if 3D, we create a 4x4 homogeneous transformation matrix
    if len(r) == 3:
        worldMatrix = [
            r[0][0], r[0][1], r[0][2], 0,
            r[1][0], r[1][1], r[1][2], 0,
            r[2][0], r[2][1], r[2][2], 0,
            o[0], o[1], o[2], 1
        ]

    # if 2D, we create a 3x3 homogeneous transformation matrix
    if len(r) == 2:
        worldMatrix = [
            r[0][0], r[0][1], 0,
            r[1][0], r[1][1], 0,
            o[0], o[1], 1
        ]

    # nesting the matrix values into object with @value props
    for i in range(0, len(worldMatrix)):
        # worldMatrix[i] = {"@value": float(worldMatrix[i])}
        worldMatrix[i] = float(worldMatrix[i])

    resource.worldMatrix = worldMatrix


    resource.resolution = {
        "value": r[0][0],
        "unitCode": config["sampling_space_unit"]
    }
    return resource
