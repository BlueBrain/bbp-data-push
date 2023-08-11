"""push modules common functions"""
import os
import json
import jwt
import hashlib
import re
from kgforge.core import Resource

# Constants
atlasrelaseType = "BrainAtlasRelease"
meTypeDensity = "METypeDensity"
parcellationType = "BrainParcellationDataLayer"
hemisphereType = "HemisphereAnnotationDataLayer"
ontologyType = "ParcellationOntology"
placementHintsType = "PlacementHintsDataLayer"
cellOrientationType = "CellOrientationField"

all_types = {
    meTypeDensity: [meTypeDensity, "NeuronDensity", "VolumetricDataLayer", "CellDensityDataLayer"],
    parcellationType: [parcellationType, "VolumetricDataLayer", "Dataset"],
    hemisphereType: [hemisphereType, "VolumetricDataLayer", "Dataset"],
    ontologyType: [ontologyType, "Ontology", "Entity"],
    atlasrelaseType: [atlasrelaseType, "AtlasRelease", "Entity"],
    placementHintsType: [placementHintsType, "VolumetricDataLayer", "Dataset"],
    cellOrientationType: [cellOrientationType, "VolumetricDataLayer", "Dataset"]
}


file_config = {
    "sampling_space_unit": "um",
    "sampling_period": 30,
    "sampling_time_unit": "ms"}


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
        "@label": "Brain Atlas Pipeline Executor role"}
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


def resolve_cellType(forge, t, name=None):
    cellType = {
        "@id": None,
        "label": None,
        # "prefLabel": "" # to define
    }
    res = forge_resolve(forge, t, name, "CellType")
    if res:
        cellType["@id"] = res.id
        cellType["label"] = res.label
    return cellType


def get_layer(forge, label):
    layer = []
    initial = "L"
    if re.match("^" + re.escape(initial) + "(\d){1,}_", label):
        layers = label.split("_")[0]
        layers_digits = layers[1:]  # 'L23' for instance
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
