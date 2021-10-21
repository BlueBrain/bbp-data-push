"""push modules common functions"""
import os
import json
import copy
import requests
import jwt
from kgforge.core import Resource


# Simplify it ? resolve automatically the voxel_type ?
def get_voxel_type(voxel_type, component_size: int):
    """
    Check if the voxel_type value is compatible with the component size or return a
    default value if voxel_type is None.

    Parameters:
        voxel_type : voxel type (string).
        component_size : integer indicating the number of component per voxel.

    Returns:
        voxel_type : str value of voxel_type is returned. Equal to the input value if
                     its value does not trigger error or else the default hardcoded
                     value is returned.
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
                    f"There is an incompatibility between the provided type "
                    f"({voxel_type }) and the component size ({component_size}) "
                    "aka the number of component per voxel."
                )
        except KeyError as e:
            raise KeyError(f"{e}. The voxel type {voxel_type} is not correct.")


def append_provenance_to_description(provenances: list, module_tag: str) -> str:
    """
    Check if the input provenance is coherent with the module_tag. If no error is
    raised, construct and return a description string displaying the Atlas pipeline
    module used and the version found in 'provenances'.

    Parameters:
        provenances : input string containing the Atlas pipeline module used and its
                      version.
        module_tag : string flag indicating which Atlas pipeline module should be used.

    Returns:
        prov_description : description string displaying the module and the version
    corresponding to the input 'provenances' tag.
    """
    module_found = False
    for provenance in provenances:
        try:
            module, module_version = provenance.split(":", 1)
            app, version = module_version.split("version ", 1)
            # if version[-1] == ",":
            #     version = version[:-1]
            if module_tag in module:
                prov_description = (
                    f"Generated in the Atlas Pipeline by the module '{module}' "
                    f"version {version}."
                )
                module_found = True
        except ValueError as e:
            raise ValueError(
                f"{e}. The provided provenance string argument must be of the "
                "form '<module_name>:<anything> <version>'."
            )
    if not module_found:
        raise ValueError(
            f"Input 'provenance' string '{provenance}' does not contain the right "
            f"module name. The correct module should contain {module_tag} in its name"
        )
    return prov_description


def get_brain_region_name_allen(region_id):
    """
    Get from the Allen Mouse Brain Atlas API (from the Allen Institute for Brain
    Science, AIBS) the region name corresponding to a region ID.

    Parameters:
        region_id : input mouse brain region identifier (integer).

    Returns:
        brain_region_info["name"] : name of the region with region_id as identifier
                                    (string).
    """
    url_base = "http://api.brain-map.org/api/v2/data/Structure/"
    response = requests.get(f"{url_base}{str(region_id)}")
    response_parsed = json.loads(response.text)

    if (
        response.status_code >= 300
        or not response_parsed["success"]
        or response_parsed["num_rows"] == 0
    ):
        return None  # TODO raise an error ?

    brain_region_info = response_parsed["msg"][0]
    return brain_region_info["name"]


def get_hierarchy_file(input_hierarchy: list, config_content: dict, hierarchy_tag: str):
    """
    If present, return the right hierarchy json file corresponding to the hierarchy_tag.
    If not, raises an error.

    Parameters:
        input_hierarchy : path to the hierarchy json file containing brain regions
                          hierarchy.
        config_content : content of the configuration yaml file containing the names
                         and paths of the atlas-pipeline generated datasets.
        hierarchy_tag : string flag indicating the hierarchy json file used.

    Returns:
        hierarchy_path : path to the right hierarchy json file contained in
                         config_content.
    """
    hierarchy_path = None
    try:
        for hierarchy_file in input_hierarchy:
            if os.path.samefile(
                hierarchy_file, config_content["HierarchyJson"][hierarchy_tag]
            ):
                hierarchy_path = hierarchy_file
    except KeyError as e:
        raise KeyError(f"KeyError: {e}")
    if not hierarchy_path:
        raise KeyError(
            f"The right hierarchy file is not among those given as input. "
            "According to the configuration file and the hierarchy tag associated "
            "with the dataset, the hierarchy file path : "
            f"'{config_content['HierarchyJson'][hierarchy_tag]}' should "
            "be provided as input"
        )

    return hierarchy_path


def get_brain_region_name(region_id: int, hierarchy_path, flat_tree: dict = None):
    """
    Search and return the region name corresponding to the input region identifier in
    the input hierarchy file. In order to do this, an array tree structure will be
    indexed as a tree structure from the brain region hierarchy file nested structure.
    This hierarchy tree is return as well to be reused the next time this function is
    called.

    Parameters:
        region_id : input mouse brain region identifier (integer).
        hierarchy_path : path to the hierarchy json file containing brain regions
                         hierarchy.
        flat_tree : the eventual hierarchy tree array indexed from the hierarchy file
                    nested content.

    Returns:
        region_name : List of one or multiple Resource object composed of attached
                      input file and their set of properties.
        hierarchy: hierarchy tree array indexed from the hierarchy file nested content.
    """
    region_name = None
    try:
        region_id = int(region_id)
    except ValueError as e:
        raise ValueError(f"ValueError: {e}")
    if not flat_tree:
        with open(hierarchy_path, "r") as hierarchy_file:
            try:
                hierarchy = json.load(hierarchy_file)
            except ValueError as e:
                raise ValueError(
                    f"Error when decoding the hierarchy json file "
                    f"'hierarchy_file'. {e}"
                )
            try:
                hierarchy = hierarchy["msg"][0]
            except KeyError:
                raise KeyError(
                    "Wrong input hierarchy file content. The AIBS hierarchy json "
                    "file dict-structure is expected."
                )
            tree_copy = copy.deepcopy(hierarchy)
            root_node = tree_copy
            flat_tree = {}
            node_to_explore = [root_node]
            while len(node_to_explore):
                node = node_to_explore.pop()
                node_id = node["id"]
                flat_tree[node_id] = node
                children_ids = []
                if "children" in node:
                    for child in node["children"]:
                        children_ids.append(child["id"])
                        node_to_explore.append(child)
                node["children"] = children_ids
            for node_id in flat_tree:
                node = flat_tree[node_id]
                node_to_explore = [] + node["children"]
                while len(node_to_explore):
                    child_id = node_to_explore.pop()
                    child_node = flat_tree[child_id]
                    node_to_explore = node_to_explore + child_node["children"]
    try:
        hierarchy = flat_tree
        region_name = hierarchy[region_id]["name"]
    except KeyError:
        raise KeyError(
            f"Region name corresponding to id '{region_id}' is not found in the "
            f"hierarchy json file ({hierarchy_path})."
        )
        # region_name = get_brain_region_name_allen(region_id) #if no resultat in the
        # hierarchy file

    return region_name, hierarchy


# # No more used
# def getExtraValues (resource, extra_keys_values):
#     """

#     Parameters:
#         resource : Resource object defined by a properties payload linked to a file.
#         extra_keys_values :

#     Returns:
#         resource :
#     """
#     extra_informations = {}
#     for t in extra_keys_values:
#         extra_informations[t[0]] = t[1]
#     resource.extra_informations = extra_informations

#     return resource


def add_contribution(forge):
    """
    Create and return a Contribution Resource from the user informations extracted
    from its token.
    To do this, the user Person Resource identifier is retrieved from Nexus if it
    exists, otherwise create a Person Resource with the user informations.
    Parameters:
        forge : instantiated and configured forge object.
        resource : Resource object defined by a properties payload linked to a file.

    Returns:
        contribution : Resource object of Contribution type. Constructed from the user
        informations.
    """
    try:
        token_info = jwt.decode(forge._store.token, options={"verify_signature": False})
    except Exception as e:
        raise Exception(f"Error when decoding the token. {e}")
    user_name = token_info["name"]
    user_family_name = token_info["family_name"]
    user_given_name = token_info["given_name"]
    user_email = token_info["email"]
    log_info = []
    try:
        user_resource = forge.resolve(
            user_family_name, target="agents", scope="agent", type="Person"
        )
    except Exception as e:
        raise Exception(
            "Error when resolving the Person Resource in the agent bucket project. "
            f"{e}"
        )
    if not user_resource:
        log_info.append(
            f"\nThe user {user_name} extracted from the user token did not "
            "correspond to an agent registered in the 'agents' project in Nexus."
        )
        try:
            p = forge.paths("Dataset")
            user_resource = forge.search(p.name == user_name, limit=1)
        except Exception as e:
            raise Exception(
                "Error when searching the user Person Resource in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )

        if user_resource:
            log_info.append(
                "A Person Resource with this user name has been found in the "
                f"project '{forge._store.bucket}'. It will be used for the "
                "contribution section."
            )
            contributor = user_resource[0]
        else:
            log_info.append(
                "No Person Resources with this user name have been found in the "
                f"'{forge._store.bucket}' project either. Thus, a Person-type "
                "resource will be created with user's information and added as "
                "contributor in the dataset payload."
            )
            contributor = Resource(
                type=["Agent", "Person"],
                name=user_name,
                familyName=user_family_name,
                givenName=user_given_name,
            )
            if user_email:
                contributor.user_email = user_email
            try:
                forge.register(contributor, "https://neuroshapes.org/dash/person")
            except Exception as e:
                raise Exception(
                    "Error when registering the user Person-type resource into "
                    f"Nexus. {e}"
                )
    else:
        # If multiple agents have the same family_name
        if isinstance(user_resource, list):
            # TO DO, or wait for future resolver update
            pass
        else:
            contributor = user_resource[0]

    agent = {"@id": contributor.id, "@type": contributor.type}
    hadRole = {
        "@id": "nsg:BrainAtlasPipelineExecutionRole",
        "label": "Brain Atlas Pipeline Executor role",
    }
    contribution_contributor = Resource(
        type="Contribution", agent=agent, hadRole=hadRole
    )
    # contribution = Resource(type="Contribution", agent=contributor)
    # my_derived_dataset.add_contribution(john.id, versioned=False)

    # Add the Agent Organization
    try:
        institution = forge.resolve(
            "École Polytechnique Fédérale de Lausanne",
            target="agents",
            scope="agent",
            type="Organization",
        )
    except Exception as e:
        raise Exception(
            "Error when resolving the Organization Resource in the agent bucket "
            f"project. {e}"
        )
    if not institution:
        try:
            filters = {"name": "École Polytechnique Fédérale de Lausanne"}
            institution = forge.search(filters, limit=1)
        except Exception as e:
            raise Exception(
                "Error when searching the Organization Resource in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
        if not institution:
            try:
                institution = forge.retrieve(
                    "https://www.grid.ac/institutes/grid.5333.6"
                )
            except Exception as e:
                raise Exception(
                    "Error when retrieving the Organization Resource "
                    "@id : "
                    "https://www.grid.ac/institutes/grid.5333.6"
                    "in the destination "
                    f"project '{forge._store.bucket}'. {e}"
                )
            if not institution:
                log_info.append(
                    "The Organization resource 'École Polytechnique Fédérale de "
                    "Lausanne' has not been found in the destination project "
                    f"'{forge._store.bucket}'. It will therefore be created."
                )
                institution = Resource(
                    id="https://www.grid.ac/institutes/grid.5333.6",
                    type="Organization",  # Agent
                    alternateName="EPFL",
                    name="École Polytechnique Fédérale de Lausanne",
                )
                try:
                    forge.register(
                        institution, "https://neuroshapes.org/dash/organization"
                    )
                except Exception as e:
                    raise Exception(
                        "Error when registering the user Organization-type resource "
                        f"into Nexus. {e}"
                    )
        else:
            institution = institution[0]

    agent = {"@id": institution.id, "@type": "Organization"}  # Agent
    contribution_institution = Resource(
        type="Contribution",
        agent=agent,
    )

    contribution = [contribution_contributor, contribution_institution]

    return contribution, log_info
