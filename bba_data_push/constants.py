"""
All the constants and hardcoded stuff.
Update the content of the dictionnary accordingly with the dataset names from the
input configuration file '--config-path'.
"""


# ================== Commons constants ==================

# Allen annotation volume voxels resolution in microns
VOXELS_RESOLUTION = "25"
SPATIAL_UNIT = "µm"
dataset_type = "Dataset"
subject = {
    "@type": "Subject",
    "species": {
        "@id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
        "label": "Mus musculus",
    },
}
atlas_reference_system_id = (
    "https://bbp.epfl.ch/neurosciencegraph/data/allen_ccfv3_spatial_reference_system"
)
# Link to the spatial ref system
isRegisteredIn = {
    "@type": ["BrainAtlasSpatialReferenceSystem", "AtlasSpatialReferenceSystem"],
    "@id": atlas_reference_system_id,
}
# Descriptions
description_ccfv2 = (
    f"original Allen ccfv2 annotation volume at {VOXELS_RESOLUTION} {SPATIAL_UNIT}"
)
description_ccfv3 = (
    f"original Allen ccfv3 annotation volume at {VOXELS_RESOLUTION} {SPATIAL_UNIT}"
)
description_hybrid = (
    f"Hybrid annotation volume from ccfv2 and ccfv3 at {VOXELS_RESOLUTION} "
    f"{SPATIAL_UNIT}"
)
description_split = "with the isocortex layer 2 and 3 split"
description_ccfv3_split = f"{description_ccfv3} {description_split}"
description_hybrid_split = f"{description_hybrid} {description_split}"

# Nexus schema

schema_ontology = "https://neuroshapes.org/dash/ontology"
schema_atlasrelease = "https://neuroshapes.org/dash/atlasrelease"
schema_activity = "https://neuroshapes.org/dash/activity"
schema_volumetricdatalayer = "https://neuroshapes.org/dash/volumetricdatalayer"
schema_mesh = "https://neuroshapes.org/dash/brainparcellationmesh"
schema_cellrecord = "https://neuroshapes.org/dash/cellrecordseries"
schema_regionsummary = ""  # https://neuroshapes.org/dash/entity

# atlasRelease already in Nexus bbp/atlas project
atlasrelease_ccfv2 = {
    "@id": (
        "https://bbp.epfl.ch/neurosciencegraph/data/"
        "dd114f81-ba1f-47b1-8900-e497597f06ac"
    ),
    "@type": ["AtlasRelease", "BrainAtlasRelease", "Entity"],
}
atlasrelease_ccfv3 = {
    "@id": (
        "https://bbp.epfl.ch/neurosciencegraph/data/"
        "831a626a-c0ae-4691-8ce8-cfb7491345d9"
    ),
    "@type": ["AtlasRelease", "BrainAtlasRelease", "Entity"],
}
atlasrelease_ccfv2v3 = [atlasrelease_ccfv2, atlasrelease_ccfv3]
atlasrelease_hybrid_l23split = {
    "@id": (
        "https://bbp.epfl.ch/neurosciencegraph/data/"
        "e2e500ec-fe7e-4888-88b9-b72425315dda"
    ),
    "@type": ["AtlasRelease", "BrainAtlasRelease", "Entity"],
}

# ================== Ontology constants ==================

hierarchy_dict = {
    "hierarchy_mba": {"name": "hierarchy", "mba_jsonld": ""},
    "hierarchy_l23split": {
        "name": "hierarchy_l23split",
        "label": "AIBS Mouse CCF Atlas parcellation ontology L2L3 split",
        "description": "AIBS Mouse CCF Atlas regions hierarchy tree file including the "
        "split of layer 2 and layer 3",
        "derivation": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
        "mba_jsonld": "mba_hierarchy_l23split",
    },
}

# ================== atlasRelease constants ==================

# Parcellations used by atlasReleases
annotation_hybrid_l23split = "annotation_hybrid_l23split"
annotation_ccfv3_l23split = "annotation_ccfv3_l23split"

# average brain model ccfv3
brainTemplateDataLayer = {
    "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
    "dca40f99-b494-4d2c-9a2f-c407180138b7",
    "@type": "BrainTemplateDataLayer",
}

atlasrelease_dict = {
    "atlasrelease_hybridsplit": {
        "name": "Allen Mouse CCF v2-v3 hybrid l2-l3 split",
        "description": "This atlas release uses the brain parcellation resulting of "
        "the hybridation between CCFv2 and CCFv3 and integrating the splitting of "
        "layer 2 and layer 3. The average brain template and the ontology is "
        "common across CCFv2 and CCFv3.",
        "ontology": hierarchy_dict["hierarchy_l23split"],
        "parcellation": annotation_hybrid_l23split,
    },
    "atlasrelease_ccfv3split": {
        "name": "Blue Brain Mouse Atlas",  # "Allen Mouse CCF v3 l2-l3 split",
        "description": "This atlas release uses the brain parcellation of CCFv3 (2017) "
        "with the isocortex layer 2 and 3 split. The average brain template and the "
        "ontology is common across CCFv2 and CCFv3.",
        "ontology": hierarchy_dict["hierarchy_l23split"],
        "parcellation": annotation_ccfv3_l23split,
    },
}

# ================== VolumetricDataLayer constants ==================

volumetric_type = "VolumetricDataLayer"
ontology_type = "ParcellationOntology"
default_sampling_period = 30
default_sampling_time_unit = "ms"


def return_volumetric_dict(volumetric_datasets):
    """
    Parameters:
        volumetric_datasets : Dict containing all the volumetric datasets from the
                            input config file.

    Returns:
        volumetric_dict : Dict containing all the volumetric datasets with their
                        informations.
    """
    # Descriptions for VolumetricDataLayer datasets
    description_dirvectors_ccfv3 = (
        f"3D unit vectors defined over the Original Allen ccfv3 annotation volume "
        f"(spatial resolution of {VOXELS_RESOLUTION} {SPATIAL_UNIT}) and representing "
        "the neuron axone-to-dendrites orientation to voxels from the top regions of "
        "the Isocortex."
    )
    description_orientation = "Quaternions field (w,x,y,z) defined over the"
    description_orientation_end = (
        f"(spatial resolution of {VOXELS_RESOLUTION} {SPATIAL_UNIT}) and representing "
        "the neuron axone-to-dendrites orientation to voxels from the Isocortex region."
    )
    description_orientation_ccfv3 = (
        f"{description_orientation} Original Allen ccfv3 annotation volume "
        f"{description_orientation_end}"
    )
    description_orientation_hybrid = (
        f"{description_orientation} CCF v2-v3 Hybrid annotation volume "
        f"{description_orientation_end}"
    )
    description_PH = (
        "The layers are ordered with respect to depth, which means that the layer "
        "which is the closest from the skull is the first layer (upper layer) and the "
        "deepest one is the last (lower layer)."
    )
    description_PH_ccfv3_split = (
        "Placement hints (cortical distance of voxels to layer boundaries) of the "
        f"Isocortex Layer XX of the {description_ccfv3_split}. {description_PH}"
    )
    description_PH_hybrid_split = (
        "Placement hints (cortical distance of voxels to layer boundaries) of the "
        f"Isocortex Layer XX of the {description_hybrid_split}. {description_PH}"
    )

    derivation_correctednissl = {
        "@type": "Derivation",
        "entity": {
            "@id": "nissl_corrected_volume",
            "@type": "Dataset",
        },
    }

    # Dictionary containing the possible volumetric dataset to push
    linprog = "inhibitory_neuron_densities_linprog_ccfv2_correctednissl"
    preserveprop = "inhibitory_neuron_densities_preserveprop_ccfv2_correctednissl"
    cell_density = "overall_cell_density_ccfv2_correctednissl"
    volumes = volumetric_datasets
    try:
        volumetric_dict = {
            "parcellations": {
                f"{volumes['annotation_hybrid']}": {
                    "name": "annotation_hybrid",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "BrainParcellationDataLayer",
                    ],
                    "description": f"{description_hybrid}. The version "
                    "replaces the leaf regions in ccfv3 with the leaf region of "
                    "ccfv2, which have additional levels of hierarchy.",
                    "atlasrelease": atlasrelease_ccfv2v3,
                    "voxel_type": "label",
                    "datasamplemodality": "parcellationId",
                },
                f"{volumes[annotation_hybrid_l23split]}": {
                    "name": annotation_hybrid_l23split,
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "BrainParcellationDataLayer",
                    ],
                    "description": description_hybrid_split,
                    "atlasrelease": atlasrelease_hybrid_l23split,
                    "voxel_type": "label",
                    "datasamplemodality": "parcellationId",
                },
                f"{volumes[annotation_ccfv3_l23split]}": {
                    "name": annotation_ccfv3_l23split,
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "BrainParcellationDataLayer",
                    ],
                    "description": description_ccfv3_split,
                    "atlasrelease": "atlasrelease_ccfv3split",
                    "voxel_type": "label",
                    "datasamplemodality": "parcellationId",
                },
            },
            "cell_orientations": {
                f"{volumes['direction_vectors_isocortex_ccfv3']}": {
                    "name": "direction_vectors_isocortex_ccfv3",
                    "type": [dataset_type, volumetric_type, "CellOrientationField"],
                    "description": description_dirvectors_ccfv3,
                    "atlasrelease": "atlasrelease_ccfv3split",
                    "voxel_type": "vector",
                    "datasamplemodality": "eulerAngle",
                },
                f"{volumes['cell_orientations_ccfv3']}": {
                    "name": "cell_orientations_ccfv3",
                    "type": [dataset_type, volumetric_type, "CellOrientationField"],
                    "description": description_orientation_ccfv3,
                    "atlasrelease": "atlasrelease_ccfv3split",
                    "voxel_type": "vector",
                    "datasamplemodality": "quaternion",
                },
                f"{volumes['cell_orientations_hybrid']}": {
                    "name": "cell_orientations_hybrid",
                    "type": [dataset_type, volumetric_type, "CellOrientationField"],
                    "description": description_orientation_hybrid,
                    "atlasrelease": atlasrelease_ccfv2v3,
                    "voxel_type": "vector",
                    "datasamplemodality": "quaternion",
                },
            },
            "placement_hints": {
                f"{volumes['placement_hints_hybrid_l23split']}": {
                    "name": "placement_hints_hybrid_l23split",
                    "type": [dataset_type, volumetric_type, "PlacementHintsDataLayer"],
                    "type_2": [
                        dataset_type,
                        volumetric_type,
                        "PlacementHintsDataReport",
                    ],
                    "description": description_PH_hybrid_split,
                    "atlasrelease": atlasrelease_hybrid_l23split,
                    "datasamplemodality": "distance",
                    "datasamplemodality_2": "mask",
                    "voxel_type": "vector",
                    "voxel_type_2": "label",
                    "suffixe": "CCF v2-v3 Hybrid L23 Split",
                },
                f"{volumes['placement_hints_ccfv3_l23split']}": {
                    "name": "placement_hints_ccfv3_l23split",
                    "type": [dataset_type, volumetric_type, "PlacementHintsDataLayer"],
                    "type_2": [
                        dataset_type,
                        volumetric_type,
                        "PlacementHintsDataReport",
                    ],
                    "description": description_PH_ccfv3_split,
                    "atlasrelease": "atlasrelease_ccfv3split",
                    "datasamplemodality": "distance",
                    "datasamplemodality_2": "mask",
                    "voxel_type": "vector",
                    "voxel_type_2": "label",
                    "suffixe": "CCFv3 L23 Split",
                },
            },
            "volume_mask": {
                f"{volumes['brain_region_mask_ccfv3_l23split']}": {
                    "name": "brain_region_mask_ccfv3_l23split",
                    "type": [dataset_type, volumetric_type, "BrainParcellationMask"],
                    "description": description_ccfv3_split,
                    "atlasrelease": "atlasrelease_ccfv3split",
                    "voxel_type": "label",
                    "datasamplemodality": "parcellationId",
                    "hierarchy_tag": "hierarchy_l23split",
                    "suffixe": "CCFv3 L23 Split",
                }
            },
            "cell_densities": {
                f"{volumes['cell_densities_hybrid']}": {
                    "name": "cell_densities_hybrid",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": description_hybrid,
                    "derivation": None,
                    "atlasrelease": atlasrelease_hybrid_l23split,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes['neuron_densities_hybrid']}": {
                    "name": "neuron_densities_hybrid",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": description_hybrid,
                    "derivation": None,
                    "atlasrelease": atlasrelease_hybrid_l23split,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes[cell_density]}": {
                    "name": cell_density,
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated using "
                    "the corrected nissl volume",
                    "derivation": derivation_correctednissl,
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes['cell_densities_ccfv2_correctednissl']}": {
                    "name": "cell_densities_ccfv2_correctednissl",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated using "
                    "the corrected nissl volume",
                    "derivation": f"{volumes[cell_density]}",
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes['neuron_densities_ccfv2_correctednissl']}": {
                    "name": "neuron_densities_ccfv2_correctednissl",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated using "
                    "the corrected nissl volume",
                    "derivation": (
                        f"{volumes['cell_densities_ccfv2_correctednissl']}",
                        "neuron_density.nrrd",
                    ),
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes[linprog]}": {
                    "name": linprog,
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated with "
                    "the corrected nissl volume and using the algorithm linprog",
                    "derivation": (
                        f"{volumes['cell_densities_ccfv2_correctednissl']}",
                        "neuron_density.nrrd",
                    ),
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes[preserveprop]}": {
                    "name": preserveprop,
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated with "
                    "the corrected nissl volume and using the algorithm "
                    "keep-proportions",
                    "derivation": (
                        f"{volumes['cell_densities_ccfv2_correctednissl']}",
                        "neuron_density.nrrd",
                    ),
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes['mtypes_densities_profile_ccfv2_correctednissl']}": {
                    "name": "mtypes_densities_profile_ccfv2_correctednissl",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated from "
                    "density profiles and using the corrected nissl volume",
                    "derivation": None,
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
                f"{volumes['mtypes_densities_probability_map_ccfv2_correctednissl']}": {
                    "name": "mtypes_densities_probability_map_ccfv2_correctednissl",
                    "type": [
                        dataset_type,
                        volumetric_type,
                        "CellDensityDataLayer",
                        "GliaCellDensity",
                    ],
                    "description": f"{description_ccfv2}. It has been generated from a "
                    "probability mapping and using the corrected nissl volume",
                    "derivation": None,
                    "atlasrelease": atlasrelease_ccfv2,
                    "voxel_type": "intensity",
                    "datasamplemodality": "quantity",
                },
            },
        }
    except KeyError as error:
        raise KeyError(
            f"KeyError: {error} does not correspond to one of the datasets defined in"
            "the VolumetricFile section of the 'generated dataset' configuration file."
        )
        exit(1)

    return volumetric_dict


def return_mesh_dict(mesh_datasets):
    """
    Parameters:
        mesh_datasets : Dict containing all the mesh datasets from the input config
                        file.

    Returns:
        mesh_dict : Dict containing all the mesh datasets with their informations.
    """
    mesh = mesh_datasets
    try:
        mesh_dict = {
            f"{mesh['brain_region_meshes_hybrid']}": {
                "name": "brain_region_meshes_hybrid",
                "description": description_hybrid,
                "atlasrelease": atlasrelease_ccfv2v3,
                "hierarchy_tag": hierarchy_dict["hierarchy_mba"]["name"],
                "annotation_name": "Hybrid",
                "derivation_type": [
                    "VolumetricDataLayer",
                    "BrainParcellationMask",
                    "Dataset",
                ],
            },
            f"{mesh['brain_region_meshes_hybrid_l23split']}": {
                "name": "brain_region_meshes_hybrid",
                "description": description_hybrid_split,
                "atlasrelease": atlasrelease_hybrid_l23split,
                "hierarchy_tag": hierarchy_dict["hierarchy_l23split"]["name"],
                "annotation_name": "Hybrid L23split",
                "derivation_type": [
                    "VolumetricDataLayer",
                    "BrainParcellationMask",
                    "Dataset",
                ],
            },
            f"{mesh['brain_region_meshes_ccfv3_l23split']}": {
                "name": "brain_region_meshes_ccfv3_l23split",
                "description": description_ccfv3_split,
                "atlasrelease": "atlasrelease_ccfv3split",
                "hierarchy_tag": hierarchy_dict["hierarchy_l23split"]["name"],
                "annotation_name": "CCFv3 L23split",
                "derivation_type": [
                    "VolumetricDataLayer",
                    "BrainParcellationMask",
                    "Dataset",
                ],
            },
        }
    except KeyError as error:
        raise KeyError(
            f"KeyError: {error} does not correspond to one of the datasets defined in "
            "the MeshFile section of the 'generated dataset' configuration file."
        )
        exit(1)

    return mesh_dict


def return_metadata_dict(metadata_datasets):
    """
    Parameters:
        metadata_datasets : Dict containing all the metadata json datasets from the
                           input config file.

    Returns:
        metadata_dict : Dict containing all the metadata json datasets with their
                        informations.
    """
    metadata = metadata_datasets
    try:
        metadata_dict = {
            f"{metadata['metadata_parcellations_ccfv3_l23split']}": {
                "name": "metadata_parcellations_ccfv3_l23split",
                "description": description_ccfv3_split,
                "atlasrelease": "atlasrelease_ccfv3split",
                "hierarchy_tag": "hierarchy_l23split",
                "annotation_name": "Ccfv3 L23split",
            }
        }
    except KeyError as error:
        raise KeyError(
            f"KeyError: {error} does not correspond to one of the datasets defined in "
            "the MetadataFile section of the 'generated dataset' configuration file."
        )
        exit(1)

    return metadata_dict


def return_cellrecords_dict(cellrecords_datasets):
    """
    Parameters:
        cellrecords_datasets : Dict containing all the cellrecord datasets from the
                               input config file.

    Returns:
        cellrecord_dict : Dict containing all the cellrecord datasets with their
                          informations.
    """
    cellrecords = cellrecords_datasets
    try:
        cellrecord_dict = {
            f"{cellrecords['cell_records_sonata']}": {
                "name": "cell_records_sonata",
                "description": f"Sonata .h5 file storing the 3D cell positions and "
                f"orientations of the {description_hybrid}.",
                "atlasrelease": atlasrelease_hybrid_l23split,
            }
        }
    except KeyError as error:
        raise KeyError(
            f"KeyError: {error} does not correspond to one of the datasets defined in "
            "the CellRecordsFile section of the 'generated dataset' configuration file."
        )
        exit(1)

    return cellrecord_dict
