"""push modules common functions"""
import os
import json
import copy
import requests
import jwt
import hashlib
from uuid import uuid4
from datetime import datetime
from kgforge.core import Resource
from kgforge.core.commons.exceptions import RegistrationError, RetrievalError
import bba_data_push.constants as const


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


# No more used
# def append_provenance_to_description(provenances: list, module_tag: str) -> str:
#     """
#     Check if the input provenance is coherent with the module_tag. If no error is
#     raised, construct and return a description string displaying the Atlas pipeline
#     module used and the version found in 'provenances'.

#     Parameters:
#         provenances : input string containing the Atlas pipeline module used and its
#                       version.
#         module_tag : string flag indicating which Atlas pipeline module should be
#                      used.

#     Returns:
#         prov_description : description string displaying the module and the version
#     corresponding to the input 'provenances' tag.
#     """
#     module_found = False
#     for provenance in provenances:
#         try:
#             module, module_version = provenance.split(":", 1)
#             app, version = module_version.split("version ", 1)
#             # if version[-1] == ",":
#             #     version = version[:-1]
#             if module_tag in module:
#                 prov_description = (
#                     f"Generated in the Atlas Pipeline by the module '{module}' "
#                     f"version {version}."
#                 )
#                 module_found = True
#         except ValueError as e:
#             raise ValueError(
#                 f"{e}. The provided provenance string argument must be of the "
#                 "form '<module_name>:<anything> <version>'."
#             )
#     if not module_found:
#         raise ValueError(
#             f"Input 'provenance' string '{provenance}' does not contain the right "
#             f"module name. The correct module should contain {module_tag} in its name"
#         )
#     return prov_description


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


def get_brain_region_prop(
    region_id: int, region_info: list, hierarchy_path, flat_tree: dict = None
):
    """
    Search and return the region name corresponding to the input region identifier in
    the input hierarchy file. In order to do this, an array tree structure will be
    indexed as a tree structure from the brain region hierarchy file nested structure.
    This hierarchy tree is return as well to be reused the next time this function is
    called.

    Parameters:
        region_id : input mouse brain region identifier (integer).
        region_info : input list of region information to return.
        hierarchy_path : path to the hierarchy json file containing brain regions
                         hierarchy.
        flat_tree : the eventual hierarchy tree array indexed from the hierarchy file
                    nested content.

    Returns:
        region_info_dict : Dictionary whose keys are from the input region_info and
        values are from the input hierarchy_path
        contained region_info .
        hierarchy: hierarchy tree array indexed from the hierarchy file nested content.
    """
    region_info_list = [
        "id",
        "atlas_id",
        "ontology_id",
        "acronym",
        "name",
        "color_hex_triplet",
        "graph_order",
        "st_level",
        "hemisphere_id",
        "parent_structure_id",
    ]

    for info in region_info:
        if info not in region_info_list:
            raise KeyError(
                f"Error: The region information '{info}' is not a valable one"
            )

    region_info_dict = {}
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
        for info in region_info:
            region_info_dict[info] = hierarchy[region_id][info]
    except KeyError:
        raise KeyError(
            f"Region {info} corresponding to id '{region_id}' is not found in the "
            f"hierarchy json file ({hierarchy_path})."
        )
        # region_name = get_brain_region_name_allen(region_id) #if no resultat in the
        # hierarchy file

    return region_info_dict, hierarchy


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


def return_file_hash(file_path):
    """
    Python program to find SHA256 hash string of a file.
    Read and update hash string value in blocks of 4K because sometimes won't be able
    to fit the whole file in memory = you have to read chunks of memory of 4096
    bytes sequentially and feed them to the sha256 method.

    Parameters:
        file_path : File path.

    Returns: Hash value of the input file.
    """
    sha256_hash = hashlib.sha256()  # SHA-256 hash object

    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def fetch_linked_resources(
    forge,
    atlasRelease,
    resource_type_list,
    datasamplemodality_list,
    resource_flag,
    parcellationAtlas_id=None,
):
    """
    Return the resources fetched from Nexus using the type, datasamplemodality and
    resource_flag informations. If there is only one resource to fetch, a resource
    will be returned otherwise a dictionary indexed containing all the resources will
    be returned.

    Parameters:
        forge : Instantiated and configured forge object.
        atlasRelease : dict containing atlasRelease @id and @type.
        resource_type_list : List containing the resource types to fetch.
        datasamplemodality_list : List containing the resource datasamplemodality to
                                 fetch.
        resource_flag : string flag indicating the resource to fetch.

    Returns:
        fetched_resources : Fetched resource or dict containing fetched resources.
    """
    fetched_resources = {}
    try:
        if resource_flag == "isPH":
            filters = {
                "type": resource_type_list[1],
                "atlasRelease": {"id": atlasRelease["@id"]},
                "dataSampleModality": datasamplemodality_list[1],
            }
            fetched_resources_PHreport = forge.search(filters, limit=1)[0]
            fetched_resources["report"] = fetched_resources_PHreport
            filters = {
                "type": resource_type_list[0],
                "atlasRelease": {"id": atlasRelease["@id"]},
                "dataSampleModality": datasamplemodality_list[0],
            }
            fetched_resources_PHlayer = forge.search(filters, limit=7)
            for resource in fetched_resources_PHlayer:
                layer_nbr = f"{resource.layer.label}".rsplit(" ", 1)[-1]
                fetched_resources.update[layer_nbr] = resource
        elif resource_flag == "isRegionMask":
            filters = {
                "type": resource_type_list[0],
                "atlasRelease": {"id": atlasRelease["@id"]},
            }
            fetched_resources_regionmask = forge.search(filters, limit=1500)
            if fetched_resources_regionmask:
                for resource in fetched_resources_regionmask:
                    # more memory efficient than split because does not keep all the
                    # split tokens in memory
                    region_number = resource.brainLocation.brainRegion.id.rsplit(
                        "/", 1
                    )[-1]
                    fetched_resources[f"{region_number}"] = resource
        elif resource_flag == "isRegionMesh":
            filters = {
                "type": resource_type_list[0],
                "atlasRelease": {"id": atlasRelease["@id"]},
            }
            fetched_resources_regionmesh = forge.search(filters, limit=1500)
            if fetched_resources_regionmesh:
                for resource in fetched_resources_regionmesh:
                    region_number = resource.brainLocation.brainRegion.id.rsplit(
                        "/", 1
                    )[-1]
                    fetched_resources[f"{region_number}"] = resource
        elif resource_flag == "isRegionSummary":
            filters = {
                "type": resource_type_list[0],
                "atlasRelease": {"id": atlasRelease["@id"]},
            }
            fetched_resources_regionsummary = forge.search(filters, limit=1500)
            if fetched_resources_regionsummary:
                for resource in fetched_resources_regionsummary:
                    region_number = resource.brainLocation.brainRegion.id.rsplit(
                        "/", 1
                    )[-1]
                    fetched_resources[f"{region_number}"] = resource
        elif resource_flag == "isAtlasParcellation":
            if parcellationAtlas_id:
                fetched_resources = forge.retrieve(parcellationAtlas_id + "?rev")
            else:
                filters = {
                    "type": resource_type_list[0],
                    "atlasRelease": {"id": atlasRelease["@id"]},
                }
                if datasamplemodality_list:
                    filters["dataSampleModality"] = datasamplemodality_list[0]
                fetched_resources = forge.search(filters, limit=1)[0]
        else:
            filters = {
                "type": resource_type_list[0],
                "atlasRelease": {"@id": atlasRelease["@id"]},
            }
            if datasamplemodality_list:
                filters["dataSampleModality"] = datasamplemodality_list[0]
            print("\nSearching forge with filters: ", filters)
            fetched_resources = forge.search(filters, limit=1000) # default limit=100
        print("\nlen(fetched_resources):", len(fetched_resources))
    except KeyError as error:
        raise KeyError(f"KeyError in atlasRelease dict. {error}")
    except IndexError:
        pass
    except TypeError:
        pass
    except AttributeError:
        pass
    return fetched_resources


def return_contribution(forge, cellComp=False):
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
    user_family_name = token_info.get("family_name", token_info.get("groups"))
    user_given_name = token_info.get("given_name", token_info.get("clientId"))
    user_name = token_info.get("name")
    if not user_name:
        user_name = f"{user_family_name} {user_given_name}"
    user_email = token_info.get("email")
    user_id = f"{forge._store.endpoint}/realms/bbp/users/{token_info['preferred_username']}"
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
    if cellComp or not user_resource:
        log_info.append(
            f"\nThe user {user_name} extracted from the user token did not "
            "correspond to an agent registered in the 'agents' project in Nexus."
        )
        try:
            # filters = {"name": "user_name"}
            # user_resource = forge.search(filters, limit=1)
            p = forge.paths("Dataset")
            user_resource = forge.search(p.name == user_name, limit=1)
        except Exception as e:
            raise Exception(
                "Error when searching the user Person Resource in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )

        if user_resource and not cellComp:
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
                type = ["Agent", "Person"],
                name = user_name)
            if not cellComp:
                contributor.familyName = user_family_name
                contributor.givenName = user_given_name
                if user_email:
                    contributor.user_email = user_email
            else:
                contributor.id = user_id
            if not cellComp:
                try:
                    forge.register(contributor, "https://neuroshapes.org/dash/person")
                except Exception as e:
                    raise Exception(
                        "Error when registering the user Person-type resource into "
                        f"Nexus. {e}")
    else:
        # If multiple agents have the same family_name
        if isinstance(user_resource, list):
            # TO DO, or wait for future resolver update
            pass
        else:
            contributor = user_resource[0]

    if not cellComp:
        agent = {"@id": contributor.id, "@type": contributor.type}
        hadRole = {
            "@id": "nsg:BrainAtlasPipelineExecutionRole",
            "label": "Brain Atlas Pipeline Executor role"}
    else:
        agent = forge.as_json(contributor)

    contribution_contributor = Resource(
        type="Contribution", agent=agent)
    if not cellComp:
        contribution_contributor.hadRole = hadRole
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
            filters = {
                "type": "Organization",
                "name": "École Polytechnique Fédérale de Lausanne",
            }
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
                    type=["Organization", "Agent"],
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

    contribution = [contribution_contributor]
    if not cellComp:
        contribution.append(contribution_institution)

    return contribution, log_info


def return_softwareagent(forge, metadata_dict):
    """
    Create and return a SoftwareAgent Resource from the provenance informations
    extracted from 'metadata_dict'.
    To do this, the user SoftwareAgent Resource identifier is retrieved from Nexus if
    it exists, otherwise create a SoftwareAgent Resource with the user informations.

    Parameters:
        forge : instantiated and configured forge object.
        metadata_dict : Dict containing informations about the pipeline run.

    Returns:
        softwareagent_resource : Resource object of SoftwareAgent type.
    """
    try:
        softwareagent_resources = forge.resolve(
            metadata_dict["softwareagent_name"],
            target="agents",
            scope="agent",
            type="SoftwareAgent",
        )
    except Exception:
        pass

    if softwareagent_resources:
        softwareagent_resource = softwareagent_resources[0]
    else:
        try:
            filters = {
                "type": "SoftwareAgent",
                "name": f"{metadata_dict['softwareagent_name']}",
            }
            softwareagent_resources = forge.search(filters, limit=1)
        # forge.search return nothing if it does not found the resource
        except Exception as e:
            raise Exception(
                "Error when searching the SoftwareAgent Resource in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
        if softwareagent_resources:
            softwareagent_resource = softwareagent_resources[0]
        else:
            # If no softwareAgent with the input name has been found then create it
            try:
                softwareSourceCode = {
                    "@type": "SoftwareSourceCode",
                    "codeRepository": metadata_dict["repo_adress"],
                    "programmingLanguage": metadata_dict["language"],
                }
                description = (
                    "Set of processing modules generating data for the Mouse Cell "
                    "Atlas."
                )
                softwareagent_resource = Resource(
                    type=["Agent", "SoftwareAgent"],
                    name=metadata_dict["softwareagent_name"],
                    description=description,
                    softwareSourceCode=softwareSourceCode,
                )
            except KeyError as e:
                raise KeyError(
                    f"KeyError : {e} when constructing the Activity SoftwareAgent "
                    "Resource"
                )
            try:
                forge.register(
                    softwareagent_resource, "https://neuroshapes.org/dash/softwareagent"
                )
            except Exception as e:
                raise Exception(
                    f"Error when registering the SoftwareAgent Resource into Nexus. {e}"
                )

    return softwareagent_resource


def return_activity_payload(
    forge,
    activity_metadata,
):
    """
    Create and return a Activity Resource from the provenance informations extracted
    from 'activity_metadata'.
    To do this, the user Activity Resource identifier is retrieved from Nexus if it
    exists, otherwise create a Activity Resource with the user informations.

    Parameters:
        forge : instantiated and configured forge object.
        activity_metadata : Dict containing informations about the pipeline run.

    Returns:
        activity_resource : Resource object of Activity type.
    """
    try:
        configuration = (
            "Activity generated using the snakemake rule "
            f"'{activity_metadata['rule_name']}.'"
        )
        activity_type = (
            activity_metadata["rule_name"].replace("_", " ").title().replace(" ", "")
        )
        startedAtTime = {
            "@type": "xsd:dateTime",
            "@value": f"{activity_metadata['start_time']}",
        }
    except KeyError as error:
        raise KeyError(
            f"KeyError: {error} missing in the input activity metadata json file"
        )
    try:
        activity_resource = forge.retrieve(activity_metadata["activity_id"])
    except RegistrationError:
        pass
    except RetrievalError:
        pass
    if activity_resource:
        if isinstance(activity_resource, list):
            activity_resource = activity_resource[0]
        else:
            activity_resource = activity_resource

        # when the activity Resource is fetched from Nexus, the property
        # 'value' needs to be mapped back to @value
        if hasattr(activity_resource, "startedAtTime"):
            # A Resource property that is a dict at the creation of the Resource
            # becomes a Resource attribute after being synchronized on Nexus
            if not isinstance(activity_resource.startedAtTime, dict):
                if hasattr(activity_resource.startedAtTime, "@value") and hasattr(activity_resource.startedAtTime, "type"):
                    value = getattr(activity_resource.startedAtTime, "@value")
                    type = getattr(activity_resource.startedAtTime, "type")
                    activity_resource.startedAtTime = forge.from_json({
                        "@type": type,
                        "@value": value}
                    )

    else:
        try:
            softwareagent_resource = return_softwareagent(forge, activity_metadata)
        except Exception as e:
            raise Exception(f"{e}.")

        try:
            used = []
            for dataset, metadata in activity_metadata["input_dataset_used"].items():
                if metadata["type"] == "ParcellationOntology":
                    used_data_type = ["Entity", "ParcellationOntology"]
                else:
                    used_data_type = ["Entity", "Dataset", metadata["type"]]
                entity = {"@id": metadata["id"], "@type": used_data_type}
                used.append(entity)
        except Exception as e:
            raise Exception(f"Error: {e}.")
        except RetrievalError as e:
            raise RetrievalError(
                f"Error when trying to retrieve the Resources contained in "
                f" 'input_dataset_used'. {e}."
            )
        softwaresourcecode = softwareagent_resource.softwareSourceCode
        # A Resource property that is a dict at the creation of the Resource becomes a
        # Resource attribut after being synchronized on Nexus
        if isinstance(softwaresourcecode, dict):
            softwareSourceCode = {
                "@type": softwaresourcecode["@type"],
                "codeRepository": softwaresourcecode["codeRepository"],
                "programmingLanguage": softwaresourcecode["programmingLanguage"],
                "version": activity_metadata["software_version"],
                "runtimePlatform": activity_metadata["runtime_platform"],
            }
        else:
            softwareSourceCode = {
                "@type": softwaresourcecode.type,
                "codeRepository": softwaresourcecode.codeRepository,
                "programmingLanguage": softwaresourcecode.programmingLanguage,
                "version": activity_metadata["software_version"],
                "runtimePlatform": activity_metadata["runtime_platform"],
            }
        wasAssociatedWith = [
            {
                "@type": softwareagent_resource.type,
                "@id": softwareagent_resource.id,
                "description": softwareagent_resource.description,
                "name": softwareagent_resource.name,
                "softwareSourceCode": softwareSourceCode,
            }
        ]
        activity_resource = Resource(
            id=activity_metadata["activity_id"],
            type=["Activity"],
            name=f"rule: {activity_metadata['rule_name']}",
            startedAtTime=startedAtTime,
            wasAssociatedWith=wasAssociatedWith,
            used=used,
        )
        try:
            activity_resource.type.append(activity_type)
            activity_resource.wasAssociatedWith[0]["configuration"] = configuration
        except Exception as e:
            raise Exception(
                "The Activity Resource payload does not contain all the "
                f"expected properties. {e}."
            )

    return activity_resource


def return_atlasrelease(
    forge,
    new_atlas,
    atlasrelease_config,
    atlasrelease_payloads,
    resource_tag=None,
    isSecondaryCLI=False,
):
    """
    Return a dictionary containing the atlasRelease and ontology resource. If their
    ids are found in the atlasrelease configuration json file they will be fetched and
    their payloads will be updated.

    Parameters:
        forge : Instantiated and configured forge object.
        atlasrelease_config_path : Json file meant to contain the atlasrelease and
                                   ontology @id
        atlasrelease_payloads : Dict meant to contain the atlasRelease and ontology
                                resources.
        resource_tag : String containing the tag value.

    Returns:
        atlasrelease_payloads : Fetched resource or dict containing the atlasRelease
                                and ontology resources.
    """
    releaseDate = {
        "@type": "xsd:date",
        "@value": f"{datetime.today().strftime('%Y-%m-%d')}",
    }
    atlasrelease = {"id": "", "tag": ""}
    if new_atlas:
        if not atlasrelease_config:
            raise Exception("New atlas requested but no config provided:", atlasrelease_config)
        atlasrelease_choice = list(atlasrelease_config.keys())[0]
    else:
        atlasrelease_choice = atlasrelease_payloads["atlasrelease_choice"]

    atlasrelease_payloads["atlasrelease_choice"] = atlasrelease_choice
    if ((not new_atlas)  and  isinstance(atlasrelease_payloads["atlasrelease_choice"], dict)):
        atlasrelease_payloads["aibs_atlasrelease"] = atlasrelease_choice
    else:
        atlasrelease_payloads["aibs_atlasrelease"] = False
        ontology_choice = const.atlasrelease_dict[atlasrelease_choice]["ontology"]
        ontology_id = None
        ontology_distribution = None
        ontology_derivation = None
        ontology_metadata = None
        atlasrelease_id = None
        atlas_release_metadata = None
        parcellationOntology = None
        parcellationVolume = None
        # Check the content of atlasrelease_config
        if not new_atlas:
            try:
                atlasrelease = atlasrelease_config[atlasrelease_choice]
                atlasrelease_resource = forge.retrieve(atlasrelease["@id"])
                #print("atlasrelease_resource retrieved:", atlasrelease_resource)
                if not atlasrelease_resource:
                    raise KeyError("No resource found with id %s" % atlasrelease["@id"])
                try:
                    atlasrelease_id = atlasrelease_resource.id
                    atlas_release_metadata = atlasrelease_resource._store_metadata
                    # why do we instanciate these :
                    parcellationOntology = {
                        "@id": atlasrelease_resource.parcellationOntology.id,
                        "@type": ["Entity", const.ontology_type, "Ontology"],
                    }
                    parcellationVolume = {
                        "@id": atlasrelease_resource.parcellationVolume.id,
                        "@type": ["Dataset", "BrainParcellationDataLayer"],
                    }
                    atlasrelease_payloads["fetched"] = True
                except AttributeError as error:
                    raise AttributeError(
                        f"Error with the atlasRelease resource fetched. {error}"
                    )
                try:
                    ontology_resource = forge.retrieve(
                        atlasrelease_resource.parcellationOntology.id
                    )
                    if ontology_resource:
                        ontology_id = ontology_resource.id
                        ontology_derivation = ontology_resource.derivation
                        ontology_distribution = ontology_resource.distribution
                        ontology_metadata = ontology_resource._store_metadata
                    else:
                        raise Exception(
                            "Error the ontology Resource linked to "
                            f"'{atlasrelease_resource}' has not been found destination "
                            f"project '{forge._store.bucket}'."
                        )
                except AttributeError as error:
                    raise AttributeError(
                        f"Error with the ontology resource fetched. {error}"
                    )
            except KeyError:
                print("The atlasRelease '%s' can not be found in\n%s" % (atlasrelease_choice, atlasrelease_config))
                print("\nA new atlasRelease resource ('%s') will be created and pushed into Nexus" % const.atlasrelease_dict[atlasrelease_choice]["name"])
                pass

        # ontology resource creation
        hierarchy_resource = Resource(
            type=["Entity", "Ontology", "ParcellationOntology"],
            label=ontology_choice["label"],
            description=ontology_choice["description"],
            subject=const.subject,
        )
        if ontology_id:
            hierarchy_resource.id = ontology_id
        else:
            hierarchy_resource.id = forge.format(
                "identifier", "ontologies", str(uuid4())
            )

        # If a distribution has been fetched we keep it and analyse it later
        if ontology_distribution:
            hierarchy_resource.distribution = ontology_distribution
        else:
            # Else we will create it after with the input hierarchy files
            hierarchy_resource.distribution = None
        if ontology_derivation:
            hierarchy_resource.derivation = ontology_derivation
        else:
            hierarchy_resource.derivation = {
                "@type": "Derivation",
                "entity": {
                    "@id": ontology_choice["derivation"],
                    "@type": "Entity",
                },
            }
        if ontology_metadata:
            hierarchy_resource._store_metadata = ontology_metadata

        atlasrelease_payloads["hierarchy"] = hierarchy_resource

        # atlasRelease resource creation
        spatialReferenceSystem = {
            "@id": const.atlas_spatial_reference_system_id,
            "@type": "AtlasSpatialReferenceSystem",
        }
        atlasrelease_resource = Resource(
            type=["AtlasRelease", "BrainAtlasRelease", "Entity"],
            name=const.atlasrelease_dict[atlasrelease_choice]["name"],
            description=const.atlasrelease_dict[atlasrelease_choice]["description"],
            brainTemplateDataLayer=const.brainTemplateDataLayer,
            spatialReferenceSystem=spatialReferenceSystem,
            subject=const.subject,
            releaseDate=releaseDate,
            # if None, will be modified later:
            parcellationOntology=parcellationOntology,
            parcellationVolume=parcellationVolume,
        )
        if atlasrelease_id:
            atlasrelease_resource.id = atlasrelease_id
        else:
            atlasrelease_resource.id = forge.format(
                "identifier", "brainatlasrelease", str(uuid4())
            )
        if atlas_release_metadata:
            atlasrelease_resource._store_metadata = atlas_release_metadata

        atlasrelease_payloads["atlas_release"][
            atlasrelease_choice
        ] = atlasrelease_resource

    # Tag that will be linked to the atlasRelease, its ontology, its parcellation and
    # every linked resources
    tag = ""
    if resource_tag and resource_tag != "None":
        tag = resource_tag
    elif atlasrelease.get("tag") and isSecondaryCLI:
        tag = atlasrelease["tag"]
    else:
        tag = f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}"

    atlasrelease_payloads["tag"] = tag

    return atlasrelease_payloads


def create_unresolved_payload(forge, unresolved, unresolved_dir, path=None):
    if not os.path.exists(unresolved_dir):
        os.makedirs(unresolved_dir)
    if path:
        unresolved_filename = os.path.join(unresolved_dir, path.split("/")[-2])
    else:
        unresolved_filename = os.path.join(unresolved_dir, "densities")
    print("%d unresolved resources, listed in %s" % (len(unresolved), unresolved_filename))
    with open(unresolved_filename+".json", "w") as unresolved_file:
        unresolved_file.write( json.dumps([forge.as_json(res) for res in unresolved]) )


def return_base_annotation(t):
    base_annotation = {
        "@type": [
            "Annotation",
            t+"TypeAnnotation" ],
        "hasBody": {"@type": [
            "AnnotationBody",
            t+"Type" ] },
        "name": t+"-type Annotation"
    }
    return base_annotation


def resolve_cellType(forge, t, name=None):
    cellType = {
        "@id": None,
        "label": "not_resolved",
        #"prefLabel": "" # to define
    }
    res = forge_resolve(forge, t, name)
    if res:
        cellType["@id"] = res.id
        cellType["label"] = res.label
    return cellType


def get_layer(forge, label):
    layer = []
    l = "L"
    if label.startswith(l) and "_" in label:
        layers = label.split("_")[0]
        ls = []
        if len(layers) > 1:
            ls.append(forge_resolve(forge, l+layers[1], label))
            if len(layers) > 2: # 'L23' for instance
                ls.append(forge_resolve(forge, l+layers[2], label))
        for res in ls:
            if res:
                layer.append({"@id": res.id, "label": res.label})
            else:
                layer.append({"@id": None, "label": "not_resolved"})
    return layer


def forge_resolve(forge, label, name=None):
    res = forge.resolve(label, scope="ontology", target="terms", strategy="EXACT_CASEINSENSITIVE_MATCH")
    if not res:
        from_ = "" if not name else f" from '{name}'"
        print("\nlabel '%s'%s not resolved" % (label, from_))
    else:
        if res.label.upper() != label.upper():
            print(f"\nDifferent case-insensitive resolved label: input '{label}', resolved '{res.label}'")
    return res
