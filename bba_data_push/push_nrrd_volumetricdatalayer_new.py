"""
Create a 'VolumetricDataLayer', to push into Nexus.
"""

import os
from pathlib import Path
from copy import deepcopy
import numpy as np
import nrrd

import bba_data_push.commons_new as comm
from bba_data_push.logging import create_log_handler
from kgforge.specializations.resources import Dataset

L = create_log_handler(__name__, "./push_nrrd_volumetricdatalayer.log")


def create_volumetric_resources(
        input_paths,
        dataset_type,
        atlas_release,
        forge,
        subject,
        brain_location,
        reference_system,
        contribution,
        derivation,
        L
) -> list:
    """
    Construct the input volumetric dataset that will be push with the corresponding files into Nexus as a resource.

    Parameters
    ----------
    input_paths: list
        input datasets paths. These datasets are either volumetric files or folder containing volumetric files
    dataset_type: str
        type of the Resources to build
    atlas_release: dict
        atlas release info
    forge: KnowledgeGraphForge
        instance of forge
    subject: dict
        species info
    brain_region: dict
        brain region info
    reference_system: dict
        reference system info
    contribution: list
        contributor Resources
    L: Logger
        log_handler

    Returns
    -------
    resources: list
        Resources to be pushed in Nexus.
    """

    resources = []

    extension = ".nrrd"
    file_paths = []
    for input_path in input_paths:
        if input_path.endswith(extension):
            file_paths.append(input_path)
        elif os.path.isdir(input_path):
            file_paths.extend([str(path) for path in Path(input_path).rglob("*"+extension)])

    L.info(f"{len(file_paths)} {extension} files found under '{input_paths}', creating the respective payloads...")
    for filepath in file_paths:
        filename_split = os.path.splitext(os.path.basename(filepath))
        filename = filename_split[0]

        L.info(f"\nCreating payload for '{filename}'")
        file_config = deepcopy(comm.file_config)
        file_config["file_extension"] = filename_split[1][1:]

        description = f"{filename} densities volume for the {comm.atlas_release_desc}. "
        description += comm.desc[comm.meTypeDensity]

        nrrd_resource = Dataset(forge,
            type=comm.all_types[dataset_type],
            name=filename,
            distribution=forge.attach(filepath, "application/nrrd"),
            temp_filepath = filepath,
            description=description,
            isRegisteredIn=reference_system,
            brainLocation=brain_location,
            atlasRelease=atlas_release,
            dataSampleModality=comm.type_dsm_map[dataset_type],
            subject=subject,
            contribution=contribution,
            derivation=[derivation]
        )

        L.info("Adding nrrd_props")
        try:
            header = nrrd.read_header(filepath)
            add_nrrd_props(nrrd_resource, header, file_config, "intensity")
        except nrrd.errors.NRRDError as e:
            L.error(f"NrrdError: {e}")

        L.info("Adding annotation")
        if dataset_type in [comm.meTypeDensity]:
            nrrd_resource.annotation = get_cellAnnotation(forge, filename)
            nrrd_resource.cellType = get_cellType(forge, filename)
            layer = comm.get_layer(forge, nrrd_resource.cellType[0]["label"])
            print("\nlayer", layer)
            if layer:
                nrrd_resource.brainLocation.layer = layer

        resources.append(nrrd_resource)

    return resources


def add_nrrd_props(resource, nrrd_header, config, voxel_type):
    """
    Add to the resource all the fields expected for a VolumetricDataLayer/NdRaster
    that can be found in the NRRD header.
    A resource dictionary must exist and be provided (even if empty).

    Parameters:
        resource : Resource object defined by a properties payload linked to a file.
        nrrd_header : Dict containing the input file header fields  and their
        corresponding value.
        config : Dict containing the file extension and its sampling informations.
        voxel_type : String indicating the type of voxel contained in the volumetric
        dataset.
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
        "double": "float64",
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

    # Here, 'space directions' being missing in the file, we hardcode an identity matrix
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
                space_directions = [None, [1, 0, 0], [0, 1, 0], [0, 0, 1]]  # component
            else:
                space_directions = [[1, 0, 0], [0, 1, 0], [0, 0, 1], None]  # time

        elif nrrd_header["dimension"] == 5:
            space_directions = [None, [1, 0, 0], [0, 1, 0], [0, 0, 1], None]

    resource.componentEncoding = NRRD_TYPES_TO_NUMPY[nrrd_header["type"]]
    # in case the nrrd file corresponds to a mask
    try:
        resource.endianness = nrrd_header["endian"]
    except KeyError:
        resource.endianness = "little"
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
            # this is a time dim as it is located after space dim)
            if passed_spatial_dim:
                current_dim["@type"] = "TimeDimension"
                current_dim["samplingPeriod"] = config["sampling_period"]
                current_dim["unitCode"] = config["sampling_time_unit"]
            # this is a component dim as it is located before space dim
            else:
                component_dim_index = i
                current_dim["@type"] = "ComponentDimension"
                try:
                    current_dim["name"] = comm.get_voxel_type(voxel_type,
                                                         current_dim["size"])
                except ValueError as e:
                    L.error(f"ValueError: {e}")
                    exit(1)
                except KeyError as e:
                    L.error(f"KeyError: {e}")
                    exit(1)

        resource.dimension.append(current_dim)

    # repeating the name of the component dimension in the "sampleType" base level prop
    if component_dim_index >= 0:
        resource.sampleType = resource.dimension[component_dim_index]["name"]

    # As no component dim was mentioned in metadata, it means the component is of size 1
    else:
        # prepend a dimension component
        try:
            name = comm.get_voxel_type(voxel_type, 1)
        except ValueError as e:
            L.error(f"ValueError: {e}")
            exit(1)
        component_dim = {"@type": "ComponentDimension", "size": 1, "name": name}
        resource.dimension.insert(0, component_dim)

        resource.sampleType = component_dim["name"]

    # creating the world matrix (column major)
    # 1. pynrrd creates a [nan, nan, nan] line for each 'space directions' that is
    # 'none' in the header.
    # We have to strip them off.
    worldMatrix = None
    r = []  # rotation mat
    o = space_origin
    for col in space_directions:
        if col is not None:
            r.append(col)

    # if 3D, we create a 4x4 homogeneous transformation matrix
    if len(r) == 3:
        worldMatrix = [
            r[0][0], r[0][1], r[0][2], 0,
            r[1][0], r[1][1], r[1][2], 0,
            r[2][0], r[2][1], r[2][2], 0,
            o[0], o[1], o[2], 1,
        ]

    # if 2D, we create a 3x3 homogeneous transformation matrix
    if len(r) == 2:
        worldMatrix = [r[0][0], r[0][1], 0, r[1][0], r[1][1], 0, o[0], o[1], 1]

    # nesting the matrix values into object with @value props
    for i in range(0, len(worldMatrix)):
        # worldMatrix[i] = {"@value": float(worldMatrix[i])}
        worldMatrix[i] = float(worldMatrix[i])

    resource.worldMatrix = worldMatrix

    resource.resolution = {"value": r[0][0], "unitCode": config["sampling_space_unit"]}


def get_cellAnnotation(forge, label):
    annotations = []

    types = ["M", "E"]
    cellTypes = get_cellType(forge, label)
    for i in range(len(cellTypes)):
        itype = types[i]
        annotation = comm.return_base_annotation(itype)
        annotation["hasBody"].update(cellTypes[i])

        annotations.append(annotation)

    return annotations


def get_cellType(forge, name):
    label = name.split("_densities")[0]
    parts = label.split("-")
    n_parts = len(parts)
    if n_parts > 3:
        raise Exception(
            f"Too many ({n_parts}) components identified in the density filename '"
            f"{name}': {', '.join(parts)}")
    if n_parts > 1:
        mtype = "-".join(parts[0:-1])  # account for a compound MType
        etype = parts[-1]
        me_types = [mtype, etype]
    else:
        me_types = parts

    cellTypes = []
    for t in me_types:
        cellType = comm.resolve_cellType(forge, t, name)
        cellTypes.append(cellType)

    return cellTypes
