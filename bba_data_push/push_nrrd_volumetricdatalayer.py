"""
Create a 'VolumetricDataLayer', to push into Nexus.
"""

import os
from pathlib import Path
from copy import deepcopy
import numpy as np
import nrrd

import bba_data_push.commons as comm
from bba_data_push.logging import create_log_handler
from kgforge.core import Resource
from kgforge.specializations.resources import Dataset

L = create_log_handler(__name__, "./push_nrrd_volumetricdatalayer.log")

type_attributes_map = {
    comm.meTypeDensity: {"dsm": "quantity", "voxel_type": "intensity",
        "desc": "density volume for the original Allen ccfv3 annotation at 25 um with "
            "the isocortex layer 2 and 3 split. It has been generated from a "
            "probability mapping, using the corrected nissl volume and transplanted."},
    comm.gliaDensityType: {"dsm": "quantity", "voxel_type": "intensity",
        "desc": "volume"},
    comm.neuronDensityType: {"dsm": "quantity", "voxel_type": "intensity",
        "desc": "neuron density volume"},
    comm.parcellationType: {"dsm": "parcellationId", "voxel_type": "label",
        "desc": "raster volume for brain region annotation as IDs, including the "
            "separation of cortical layers 2 and 3."},
    comm.hemisphereType: {"dsm": "parcellationId", "voxel_type": "label",
        "desc": "hemisphere annotation from Allen ccfv3 volume."},
    comm.placementHintsType: {"dsm": "distance", "voxel_type": "vector",
        "desc": "placement hints volume"},
    comm.directionVectorsType: {"dsm": "vector3D", "voxel_type": "vector",
        "desc": "direction vectors field volume"},
    comm.cellOrientationType: {"dsm": "quaternion", "voxel_type": "vector",
        "desc": "cell orientation field volume"},
    comm.brainMaskType: {"dsm": "parcellationId", "voxel_type": "label",
        "desc": "binary mask volume"}
}

me_separator = "|"


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
        L,
        res_name=None,
        flat_tree=None
) -> list:
    """
    Construct the input volumetric dataset that will be push with the corresponding files into Nexus as a resource.

    Parameters
    ----------
    input_paths: tuple
        input datasets paths. These datasets are either volumetric files or folder containing volumetric files
    dataset_type: str
        type of the Resources to build
    atlas_release: Resource
        atlas release info
    forge: KnowledgeGraphForge
        instance of forge
    subject: Resource
        species info
    brain_location: Resource
        brain region info
    reference_system: Resource
        reference system info
    contribution: list
        contributor Resources
    L: Logger
        log_handler
    res_name: str
        name to assign to the Resource
    flat_tree: dict
        flattened brain regions hierarchy

    Returns
    -------
    resources: list
        Resources to be pushed in Nexus.
    """

    attr = type_attributes_map[dataset_type]

    extension = ".nrrd"
    exc_etype = "cADpyr"
    generic_types = {
        "Generic_Inhibitory_Neuron_MType_Generic_Inhibitory_Neuron_EType": ["GIN_mtype",
                                                                            "GIN_etype"],
        "Generic_Excitatory_Neuron_MType_Generic_Excitatory_Neuron_EType": ["GEN_mtype",
                                                                            "GEN_etype"]
    }

    resources = []
    file_paths = []
    if not isinstance(input_paths, tuple):
        raise Exception(f"The 'input_paths' argument provided is not a tuple: {input_paths}")
    for input_path in input_paths:
        if input_path.endswith(extension):
            if os.path.isfile(input_path):
                file_paths.append(input_path)
        elif os.path.isdir(input_path):
            file_paths.extend([str(path) for path in Path(input_path).rglob("*"+extension)])

    tot_files = len(file_paths)
    L.info(f"{tot_files} {extension} files found under '{input_paths}', creating the respective payloads...")
    file_count = 0
    for filepath in file_paths:
        file_count += 1

        filename_split = os.path.splitext(os.path.basename(filepath))
        filename = filename_split[0]

        L.info(f"Creating payload for '{filename}' ({file_count} of {tot_files})")
        file_config = deepcopy(comm.file_config)
        file_config["file_extension"] = filename_split[1][1:]

        description = f"{filename} {attr['desc']}."

        if brain_location:
            res_brain_location = deepcopy(brain_location)
        else:
            res_brain_location = comm.create_brain_location_prop(forge, filename,
                flat_tree, reference_system)
            if dataset_type == comm.brainMaskType:
                res_name = f"Mask of {res_brain_location.brainRegion.label}"

        nrrd_resource = Dataset(forge,
            type=comm.all_types[dataset_type],
            name=res_name if res_name else filename,
            distribution=forge.attach(filepath, f"application/{extension[1:]}"),
            temp_filepath = filepath,
            temp_filename=filename,
            description=description,
            isRegisteredIn=reference_system,
            brainLocation=res_brain_location,
            atlasRelease=atlas_release,
            dataSampleModality=attr["dsm"],
            subject=subject,
            contribution=contribution,
            derivation=[derivation]
        )

        L.info("Adding nrrd_props")
        try:
            header = nrrd.read_header(filepath)
            voxel_type = attr["voxel_type"]
            if (dataset_type == comm.placementHintsType) and (header["dimension"]) < 4:
                voxel_type = "label"
            add_nrrd_props(nrrd_resource, header, file_config, voxel_type)
        except nrrd.errors.NRRDError as e:
            L.error(f"NrrdError: {e}")

        if dataset_type in [comm.meTypeDensity, comm.gliaDensityType]:
            L.info("Adding annotation")
            filename_ann = filename

            # This label extraction from filename will be dropped with https://github.com/BlueBrain/atlas-densities/pull/44
            separator = {
                comm.meTypeDensity: "_INH_densities",
                comm.gliaDensityType: "_density",}
            if dataset_type in [comm.gliaDensityType, comm.neuronDensityType]:
                filename_ann = filename_ann.capitalize()
                if dataset_type in [comm.gliaDensityType]:
                    if "Astrocyte" in filename_ann or "Olia" in filename_ann:
                        filename_ann = filename_ann.replace(separator[dataset_type], " Type"+separator[dataset_type])
            for generic_filename in generic_types:
                if generic_filename in filename:
                    filename_ann = me_separator.join([generic_types[generic_filename][0],
                                                      generic_types[generic_filename][1]])
            if exc_etype in filename_ann:
                filename_ann = filename_ann.replace(f"_{exc_etype}", f"{me_separator}{exc_etype}")

            cellTypes = get_cellType(forge, filename_ann, separator[dataset_type])
            annotation = get_cellAnnotation(cellTypes)
            nrrd_resource.annotation = Resource.from_json(annotation)
            nrrd_resource.cellType = Resource.from_json(cellTypes)

            layer = comm.get_layer(forge, nrrd_resource.cellType[0].label)
            if layer:
                nrrd_resource.brainLocation.layer = Resource.from_json(layer)
        
        if dataset_type == comm.placementHintsType:
            layer = comm.get_placementhintlayer_prop_from_name(forge, filename)
            if layer:
                nrrd_resource.brainLocation.layer = Resource.from_json(layer)
    
        L.info("Payload creation completed\n")

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


def get_cellAnnotation(cellTypes):
    annotations = []    
    types = ["M", "E"]
    for i in range(len(cellTypes)):
        itype = types[i]
        annotation = comm.return_base_annotation(itype)
        annotation["hasBody"].update(cellTypes[i])
        annotations.append(annotation)

    return annotations


def get_cellType(forge, name, separator):
    # This label extraction from filename will be dropped with https://github.com/BlueBrain/atlas-densities/pull/44
    metype_separator_excitatory = "_v3"
    target = "CellType"
    if separator == "_density":  # comm.gliaDensityType
        target = None

    label = name.split(separator)[0].split(metype_separator_excitatory)[0]
    parts = label.split(me_separator)  # split M from E type
    n_parts = len(parts)
    if n_parts > 2:
        raise Exception(
            f"Too many ({n_parts}) components identified in the density filename '"
            f"{name}': {', '.join(parts)}")
    if n_parts == 2:
        mtype = parts[0]
        etype = parts[1]
        me_types = [mtype, etype]
    else:
        me_types = parts

    cellTypes = []
    for t in me_types:
        cellType = comm.resolve_cellType(forge, t, target, name)
        cellTypes.append(cellType)

    return cellTypes
