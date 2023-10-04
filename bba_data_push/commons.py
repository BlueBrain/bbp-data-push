"""push modules common functions"""
import os
import json
import jwt
import hashlib
import re
from datetime import datetime
from kgforge.core import Resource
from kgforge.core.wrappings.paths import Filter, FilterOperator, create_filters_from_dict
from kgforge.core.commons.actions import LazyAction

import blue_brain_atlas_web_exporter.TreeIndexer as TreeIndexer

# Constants
atlasrelaseType = "BrainAtlasRelease"
meTypeDensity = "METypeDensity"
gliaDensityType = "GliaCellDensity"
neuronDensityType = "NeuronDensity"
parcellationType = "BrainParcellationDataLayer"
hemisphereType = "HemisphereAnnotationDataLayer"
ontologyType = "ParcellationOntology"
placementHintsType = "PlacementHintsDataLayer"
directionVectorsType = "DirectionVectorsField"
cellOrientationType = "CellOrientationField"
brainMeshType = "BrainParcellationMesh"
brainMaskType = "BrainParcellationMask"
volumetricType = ["VolumetricDataLayer", "Dataset"]
placementHintsDataLayerCatalogType = "PlacementHintsDataLayerCatalog"

all_types = {
    meTypeDensity: [meTypeDensity, "NeuronDensity", "VolumetricDataLayer", "CellDensityDataLayer"],
    gliaDensityType: [gliaDensityType, "VolumetricDataLayer", "CellDensityDataLayer"],
    neuronDensityType: [neuronDensityType, "VolumetricDataLayer", "CellDensityDataLayer"],
    parcellationType: [parcellationType] + volumetricType,
    hemisphereType: [hemisphereType] + volumetricType,
    ontologyType: [ontologyType, "Ontology", "Entity"],
    atlasrelaseType: [atlasrelaseType, "AtlasRelease", "Entity"],
    placementHintsType: [placementHintsType] + volumetricType,
    directionVectorsType: [directionVectorsType] + volumetricType,
    placementHintsDataLayerCatalogType: [placementHintsDataLayerCatalogType, "DataCatalog"],
    cellOrientationType: [cellOrientationType] + volumetricType,
    brainMeshType: [brainMeshType, "Mesh"],
    brainMaskType: [brainMaskType] + volumetricType
}

file_config = {
    "sampling_space_unit": "um",
    "sampling_period": 30,
    "sampling_time_unit": "ms"}

type_for_schema = {
    meTypeDensity: "VolumetricDataLayer",
    gliaDensityType: "VolumetricDataLayer",
    directionVectorsType: "VolumetricDataLayer",
    cellOrientationType: "VolumetricDataLayer",
    brainMaskType: "VolumetricDataLayer",
    hemisphereType: "VolumetricDataLayer",
    placementHintsType: "VolumetricDataLayer",
    placementHintsDataLayerCatalogType: "DataCatalog"}


def _integrate_datasets_to_Nexus(forge, resources, dataset_type, atlas_release_id, tag,
                                 logger, force_registration=False, dryrun=False):

    dataset_schema = forge._model.schema_id(type_for_schema.get(dataset_type, dataset_type))

    ress_to_update = []
    ress_to_regster = []
    filepath_update_list = []  # matching the resource list by list index
    filepath_register_list = []  # matching the resource list by list index
    resource_to_filepath = {}
    res_count = 0
    for res in resources:
        res_count += 1
        res_name = res.name
        res_msg = f"Resource '{res_name}' ({res_count} of {len(resources)})"

        res_deprecated = None
        if hasattr(res, "id") and not force_registration:
            res_id = res.id
            res_store_metadata = get_res_store_metadata(res_id, forge)
            res_deprecated = res_store_metadata._deprecated
        if (res_deprecated is not False) or force_registration:
            res_id = None
            res_store_metadata = None
            logger.info(f"Searching Nexus for {res_msg}")
            limit = 100
            filename = None
            if hasattr(res, "temp_filepath"):
                basename = os.path.basename(res.temp_filepath)
                if basename in ["[PH]y.nrrd", "Isocortex_problematic_voxel_mask.nrrd"]:
                    filename = basename
            if not force_registration:
                orig_ress, matching_filters = get_existing_resources(dataset_type, atlas_release_id, res, forge, limit, filename)
                n_orig_ress = len(orig_ress)
                if n_orig_ress > 1:
                    prefix = f"{n_orig_ress}" if n_orig_ress < limit else f"at least {limit}"
                    raise Exception(f"Error: {prefix} matching Resources found using the criteria: {matching_filters}")
                elif n_orig_ress == 1:
                    res_id = orig_ress[0].id
                    res_store_metadata = get_res_store_metadata(res_id, forge)
                else:
                    logger.info(f"No Resource found using the criteria: {matching_filters}")

        if res_id:
            res.id = res_id
            check_tag(forge, res_id, tag, logger)
            # TODO: consider to skip update if distribution SHA is identical between res and existing_res
            logger.info(f"Scheduling to update {res_msg} with Nexus id: {res_id}\n")
            setattr(res, "_store_metadata", res_store_metadata)
            if hasattr(res, "temp_filepath"):
                filepath_update_list.append(res.temp_filepath)
            else:
                filepath_update_list.append(None)
            ress_to_update.append(res)
        else:
            logger.info(f"Scheduling to register {res_msg}\n")
            if hasattr(res, "temp_filepath"):
                filepath_register_list.append(res.temp_filepath)
            else:
                filepath_register_list.append(None)
            ress_to_regster.append(res)

        if hasattr(res, "temp_filepath"):
            resource_to_filepath[res.get_identifier()] = res.temp_filepath
            delattr(res, "temp_filepath")
        if hasattr(res, "temp_filename"):
            delattr(res, "temp_filename")

    logger.info(f"Updating {len(ress_to_update)} Resources with schema '{dataset_schema}'")
    if not dryrun:
        forge.update(ress_to_update, dataset_schema)
        check_res_list(ress_to_update, filepath_update_list, "updating", logger)

    logger.info(f"Registering {len(ress_to_regster)} Resources with schema '{dataset_schema}'")
    if not dryrun:
        forge.register(ress_to_regster, dataset_schema)
        check_res_list(ress_to_regster, filepath_register_list, "registering", logger)
    else:
        for res in ress_to_regster:
            res.id = None

    ress_to_tag = ress_to_update + ress_to_regster
    filepath_tag_list = filepath_update_list + filepath_register_list
    logger.info(f"Tagging {len(ress_to_tag)} Resources with tag '{tag}'\n")
    if not dryrun:
        forge.tag(ress_to_tag, tag)
        check_res_list(ress_to_tag, filepath_tag_list, "tagging", logger)

    return resource_to_filepath

def get_placementhintlayer_prop_from_name(forge, filename):
    if "]" in filename:
        layer_label = str(filename).split(".nrrd")[0]
        layer_label = layer_label.split("]")[1]
    else:
        layer_label = filename
   
    layer_prop_resource_list = get_layer(forge, layer_label, initial="layer", regex="_(\d){1,}", split_separator=None, layer_number_offset=1) 
    return layer_prop_resource_list
    

def get_existing_resources(dataset_type, atlas_release_id, res, forge, limit, filename=None):
    filters = {"type": dataset_type,
               "atlasRelease": {"id": atlas_release_id},
               "brainLocation": {"brainRegion": {"id": res.brainLocation.brainRegion.get_identifier()}},
               "subject": {"species": {"id": res.subject.species.get_identifier()}}
               }

    def get_filters_by_type(res, type):
        filters_by_type = []
        if type == meTypeDensity:
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL, path=["annotation", "type"],
                       value=res.annotation[0].get_type()[1]))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL, path=["annotation", "type"],
                       value=res.annotation[1].get_type()[1]))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL,
                       path=["annotation", "hasBody", "id"],
                       value=res.annotation[0].hasBody.get_identifier()))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL,
                       path=["annotation", "hasBody", "id"],
                       value=res.annotation[1].hasBody.get_identifier()))
        return filters_by_type

    filter_list = create_filters_from_dict(filters)

    filter_list.extend(get_filters_by_type(res, dataset_type))

    if hasattr(res.brainLocation, "layer"):
        for layer in res.brainLocation.layer:
            filter_list.append(Filter(operator=FilterOperator.EQUAL, path=["brainLocation", "layer", "id"], value=layer.get_identifier()))

    if filename:
        filter_list.append(Filter(operator=FilterOperator.EQUAL, path=["distribution", "name"], value=filename))

    return forge.search(*filter_list, limit=limit), filter_list


def check_res_list(res_list, filepath_list, action, logger):
    error_messages = []
    for i, res in enumerate(res_list):
        if not res._last_action.succeeded:
            l_a = res._last_action
            error_messages.append(f"{res.get_identifier()},{res.name},{res.get_type()},{filepath_list[i]},"
                                  f"{l_a.error},{action},{l_a.message}")
    n_error_msg = len(error_messages)
    if n_error_msg != 0:
        errors = "\n".join(error_messages)
        logger.warning(f"Got the following {n_error_msg} errors:\n"
                       "res ID,res name,res type,filepath,error,action,message\n"
                       f"{errors}")


def check_tag(forge, res_id, tag, logger):
    logger.info(f"Verify that tag '{tag}' does not exist already for Resource id '{res_id}':")
    res = forge.retrieve(res_id, version=tag)
    if res:
        msg = f"Tag '{tag}' already exists for res id '{res_id}' (revision {res._store_metadata._rev}, Nexus address"\
              f" '{res._store_metadata._self}'), please choose a different tag."
        msg += " No resource with this schema has been tagged."
        raise Exception(msg)


def get_res_store_metadata(res_id, forge):
    res = retrieve_resource(res_id, forge)
    return res._store_metadata


def retrieve_resource(res_id, forge):
    """
    Fetch a Resource from Nexus

    Parameters
    ----------
    res_id: str
        Nexus id of the Resource to fetch
    forge: KnowledgeGraphForge
        instance of forge

    Returns
    -------
    res: Resource
        the fetched Resource
    """

    res = forge.retrieve(res_id, cross_bucket=True)
    return res


def add_distribution(res, forge, distribution):
    res_dis = list()
    for dis_file in distribution:
        res_dis.append(forge.attach(dis_file["path"], dis_file["content_type"]))
    res.distribution = res_dis if len(res_dis) > 1 else res_dis[0]


def create_brain_location_prop(forge, region_id, flat_tree, reference_system):
    region_prefix = forge.get_model_context().expand("mba")
    mba_region_id = region_prefix + region_id
    region_label = get_region_label(flat_tree, int(region_id))
    brain_region = get_property_id_label(mba_region_id, region_label)

    return get_brain_location_prop(brain_region, reference_system)


def get_brain_location_prop(brain_region, reference_system):
    return Resource(
        brainRegion=brain_region,
        atlasSpatialReferenceSystem=reference_system)


def get_flat_tree(hierarchy_path):
    hierarchy = json.loads(open(hierarchy_path).read())
    root_region = hierarchy['msg'][0]
    return TreeIndexer.flattenTree(root_region, id_prop_name="id", children_prop_name="children")


def get_region_label(flat_tree, region_id):
    region = flat_tree.get(region_id)
    if not region:
        raise Exception(f"Region id {region_id} was not found in the input hierarchy")

    return region["name"]

class Args:
    species = "species"
    brain_region = "brain-region"
    name_target_map = {species: "Species",
                       brain_region: "BrainRegion"}


def get_property_label(name, arg, forge):

    if arg.startswith("http"):
        arg_res = forge.retrieve(arg, cross_bucket=True)
    else:
        arg_res = forge.resolve(arg, scope="ontology", target=Args.name_target_map[name],
                                strategy="EXACT_MATCH")
    if not arg_res:
        raise Exception(
            f"The provided '{name}' argument ({arg}) can not be retrieved/resolved")

    return get_property_id_label(arg_res.id, arg_res.label, notation = arg_res.notation if hasattr(arg_res, "notation") else None)


def get_property_id_label(id, label, notation=None):
    prop = Resource(id=id, label=label)
    if notation:
        prop.notation = notation
    return prop


def get_property_type(arg_id, arg_type, rev=None):
    prop = Resource(id=arg_id, type=arg_type)
    if rev:
        prop._rev = rev
    return prop

def get_date_prop():
    res_dict = {"type": 'xsd:date',
                "@value": datetime.today().strftime('%Y-%m-%d')}
    return Resource.from_json(res_dict)

def get_voxel_type(voxel_type, component_size: int):
    """
    Check if the input voxel_type value is compatible with the component size.
    Return a default value if voxel_type is None.

    Parameters
    ----------
    voxel_type: str
        voxel type
    component_size: int
        number of component per voxel

    Returns
    -------
    str for voxel type
    """

    # this could be "multispectralIntensity", "vector"
    default_sample_type_multiple_components = "vector"

    # This could be "intensity", "mask", "label"
    default_sample_type_single_component = "intensity"

    allow_multiple_components = {
        "multispectralIntensity": True,
        "vector": True,
        "intensity": False,
        "mask": False,
        "label": False,
    }

    if not voxel_type and component_size == 1:
        return default_sample_type_single_component
    elif not voxel_type and component_size > 1:
        return default_sample_type_multiple_components
    elif voxel_type:
        try:
            if component_size > 1 and allow_multiple_components[voxel_type]:
                return voxel_type
            elif component_size == 1 and not allow_multiple_components[voxel_type]:
                return voxel_type
            else:
                raise ValueError(
                    f"There is an incompatibility between the provided type ("
                    f"{voxel_type}) and the component size "
                    f"({component_size}) aka the number of component per voxel.")
        except KeyError as e:
            raise KeyError(f"{e}. The voxel type {voxel_type} is not correct.")


def return_file_hash(file_path):
    """Find the SHA256 hash string of a file. Read and update hash string value in blocks of 4K because sometimes
    won't be able to fit the whole file in memory = you have to read chunks of memory of 4096 bytes sequentially
    and feed them to the sha256 method.

    Parameters
    ----------
    file_path: str
        file path

    Returns
    -------
    Hash value of the input file.
    """

    sha256_hash = hashlib.sha256()  # SHA-256 hash object

    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def return_contributor(forge, project_str, contributor_id, contributor_name, contributor_type, extra_attr, log_info):
    """
    Create and return an Agent Resource based on the information provided as arguments.

    Parameters
    ----------
    forge: KnowledgeGraphForge
        instance of forge
    project_str: str
        "org/project" of the forge instance
    contributor_id: str
        Nexus id of the contributor Resource
    contributor_name: str
        name of the contributor Resource to fetch from Nexus
    extra_attr: dict
        attributes to set in the new contributor Resource
    log_info: list
        log messages

    Returns
    -------
    contributor: Resource
        the contributor Resource fetched or created
    """

    contributor = None

    agent_type = contributor_type[0]
    if contributor_id:
        try:
            contributor_resource = forge.retrieve(contributor_id)
        except Exception as e:
            raise Exception(
                f"Error when retrieving the Resource id '{contributor_id}' from "
                f"{project_str}: {e}")
        if contributor_resource:
            contributor = contributor_resource
        else:
            extra_attr["id"] = contributor_id
    if not contributor:
        try:
            contributor_resource = forge.resolve(contributor_name, target="agents",
                                                 scope="agent", type=agent_type)
        except Exception as e:
            raise Exception(
                f"Error when resolving '{contributor_name}' in {project_str}: {e}")
        if contributor_resource:
            contributor = contributor_resource
    if contributor:
        log_info.append(
            f"A Resource for agent '{contributor_name}' has been found in the "
            f"{project_str}. "
            "It will be used for the contribution section.")
    else:
        log_info.append(
            f"\nThe agent '{contributor_name}' does not correspond to a Resource "
            f"registered in the Nexus {project_str}."
            "Thus, a Resource will be created and registered as contributor.")
        extra_attr.update({
            "type": contributor_type,
            "name": contributor_name})
        contributor = Resource.from_json(extra_attr)
        try:
            forge.register(contributor, forge._model.schema_id(agent_type))
        except Exception as e:
            raise Exception(
                f"Error when registering the Resource of type '{agent_type}' into "
                f"Nexus: {e}")

    return contributor


def return_contribution(forge, nexus_env, bucket, token, add_org_contributor=False):
    """
    Return a contribution property based on the information extracted from the token.
    When organization=True, the returned contribution contains also the Organization contributor.

    Parameters
    ----------
    forge: KnowledgeGraphForge
        instance of forge
    bucket: str
        "org/project" of the forge instance
    token: str
        token of the forge instance
    add_org_contributor: bool
        flag to add the "Organization" contributor

    Returns
    -------
        tuple of (contribution, log_info)
    """

    contribution = []
    try:
        token_info = jwt.decode(token, options={"verify_signature": False})
    except Exception as e:
        raise Exception(f"Error when decoding the token: {e}")

    user_family_name = token_info.get("family_name", token_info.get("groups"))
    user_given_name = token_info.get("given_name", token_info.get("clientId"))
    user_full_name = token_info.get("name")
    if not user_full_name:
        user_full_name = f"{user_family_name} {user_given_name}"
    user_email = token_info.get("email")
    log_info = []

    project_str = f"project '{bucket}'"

    user_id = None
    username = token_info.get('preferred_username')
    contributor_type = ["Agent"]
    if username:
        if "groups" in token_info:
            token_type = "groups"
            contributor_type.append("Organization")
        else:
            token_type = "users"
            contributor_type.append("Person")
        user_id = f"{nexus_env}/realms/bbp/{token_type}/{username}"

    extra_attr_user = {
        "familyName": user_family_name,
        "givenName": user_given_name}
    if user_email:
        extra_attr_user["user_email"] = user_email

    contributor_user = return_contributor(forge, project_str, user_id, user_full_name,
                                          contributor_type, extra_attr_user,
                                          log_info)
    agent = {"@id": contributor_user.id, "@type": contributor_user.type}
    hadRole = {
        "@id": forge.get_model_context().expand("nsg:BrainAtlasPipelineExecutionRole"),
        "label": "Brain Atlas Pipeline Executor role"}
    contribution_contributor = Resource(type="Contribution", agent=agent)
    contribution_contributor.hadRole = hadRole

    contribution.append(contribution_contributor)

    if not add_org_contributor:
        return contribution, log_info

    # Add the Agent Organization
    epfl_id = "https://www.grid.ac/institutes/grid.5333.6"
    epfl_name = "École Polytechnique Fédérale de Lausanne"
    extra_attr_org = {
        "alternateName": "EPFL"}
    contributor_org = return_contributor(forge, project_str, epfl_id, epfl_name,
                                         ["Agent", "Organization"],
                                         extra_attr_org, log_info)
    agent = {"@id": contributor_org.id, "@type": "Organization"}
    contribution_org = Resource(type="Contribution", agent=agent)
    contribution.append(contribution_org)

    return contribution, log_info


def create_unresolved_payload(forge, unresolved, unresolved_dir, path=None):
    if not os.path.exists(unresolved_dir):
        os.makedirs(unresolved_dir)
    if path:
        unresolved_filename = os.path.join(unresolved_dir, path.split("/")[-2])
    else:
        unresolved_filename = os.path.join(unresolved_dir, "densities")
    print("%d unresolved resources, listed in %s" % (
        len(unresolved), unresolved_filename))
    with open(unresolved_filename + ".json", "w") as unresolved_file:
        unresolved_file.write(json.dumps([forge.as_json(res) for res in unresolved]))


def return_base_annotation(t):
    base_annotation = {
        "@type": [
            "Annotation",
            t + "TypeAnnotation"],
        "hasBody": {"@type": [
            "AnnotationBody",
            t + "Type"]},
        "name": t + "-type Annotation"
    }
    return base_annotation


def resolve_cellType(forge, t, target, name=None):
    cellType = {
        "@id": None,
        "label": None,
        # "prefLabel": "" # to define
    }
    if target:
        res = forge_resolve(forge, t, name, target)
    else:
        res = forge_resolve(forge, t, name)
    if res:
        cellType["@id"] = res.id
        cellType["label"] = res.label
    return cellType


def get_layer(forge, label, initial="L", regex="(\d){1,}_", split_separator="_", layer_number_offset = 0):
    layer = []
    if re.match("^" + re.escape(initial) + regex, label):
        layers = label.split(split_separator)[0]
        layers_digits = layers[len(initial)+layer_number_offset:]
        initial = initial + " " if layer_number_offset > 0 else initial
        for digit in layers_digits:
            res = forge_resolve(forge, initial + digit, label, "BrainRegion")
            if res:
                layer.append(Resource(id=res.id, label=res.label))
            else:
                raise Exception(f"Layer {layers} was not found in the Knowledge graph")
    return layer


def forge_resolve(forge, label, name=None, target="terms"):
    res = forge.resolve(label, scope="ontology", target=target, strategy="EXACT_MATCH")
    if not res:
        from_ = "" if not name else f" from '{name}'"
        print("\nlabel '%s'%s not resolved" % (label, from_))
    else:
        if isinstance(res.label, str):
            if res.label.upper() != label.upper():
                print(
                    f"\nDifferent resolved label: input '{label}', resolved '"
                    f"{res.label}'")
        else:
            print("\nWARNING: The label of the resolved resource is not a string:\n",
                  res)
    return res
