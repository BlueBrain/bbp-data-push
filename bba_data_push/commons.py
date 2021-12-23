"""push modules common functions"""
import os
import json
import copy
import requests
import jwt
from uuid import uuid4
from datetime import datetime
from kgforge.core import Resource
from kgforge.core.commons.exceptions import RegistrationError


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


def return_contribution(forge):
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
        "@id": "BrainAtlasPipelineExecutionRole",
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

    contribution = [contribution_contributor, contribution_institution]

    return contribution, log_info


def return_softwareagent(forge, metadata_dict):

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
            p = forge.paths("Dataset")
            softwareagent_resources = forge.search(
                p.name == metadata_dict["softwareagent_name"], limit=1
            )
        except Exception as e:
            raise Exception(
                "Error when searching the SoftwareAgent Resource in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
        if softwareagent_resources:
            softwareagent_resource = softwareagent_resources[0]
        else:
            softwareSourceCode = {
                "@type": "SoftwareSourceCode",
                "codeRepository": metadata_dict["repo_adress"],
                "programmingLanguage": metadata_dict["langage"],
            }
            description = (
                "Set of processing modules generating data for the Mouse Cell Atlas."
            )
            softwareagent_resource = Resource(
                type=["Agent", "SoftwareAgent"],
                name=metadata_dict["softwareagent_name"],
                description=description,
                softwareSourceCode=softwareSourceCode,
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
    activity_metadata_path,
):

    try:
        with open(activity_metadata_path, "r") as f:
            activity_metadata = json.loads(f.read())
    except json.decoder.JSONDecodeError as e:
        raise (f"JSONDecodeError : {activity_metadata_path}.{e}.")
    configuration = (
        "Activity generated using the snakemake rule "
        f"'{activity_metadata['rule_name']}'."
    )
    activity_type = (
        activity_metadata["rule_name"].replace("_", " ").title().replace(" ", "")
    )
    try:
        activity_resource = forge.retrieve(activity_metadata["activity_id"])
    except RegistrationError:
        pass
    if activity_resource:
        if isinstance(activity_resource, list):
            activity_resource = activity_resource[0]
        else:
            activity_resource = activity_resource
    else:
        try:
            softwareagent_resource = return_softwareagent(forge, activity_metadata)
        except Exception as e:
            raise Exception(f"{e}.")

        try:
            used = []
            for i in range(0, len(activity_metadata["input_dataset_used"])):
                used_resource = forge.retrieve(
                    activity_metadata["input_dataset_used"][i]
                )
                if not used_resource:
                    raise Exception(
                        "Could not retrieve the 'input_dataset_used' "
                        "Resource with id "
                        f"{activity_metadata['input_dataset_used'][i]}."
                    )
                entity = {"@id": used_resource.id, "@type": used_resource.type}
                if (
                    isinstance(entity["@type"], list)
                    and "Entity" not in entity["@type"]
                ):
                    entity["@type"].append("Entity")
                if (
                    not isinstance(entity["@type"], list)
                    and entity["@type"] != "Entity"
                ):
                    entity["@type"] = [entity["@type"], "Entity"]
                used.append(entity)
        except Exception as e:
            raise Exception(
                "Error when trying to retrieve the 'input_dataset_used' "
                f"Resource. {e}."
            )
        softwareSourceCode = softwareagent_resource.softwareSourceCode
        wasAssociatedWith = [
            {
                "@type": softwareagent_resource.type,
                "@id": softwareagent_resource.id,
                "description": softwareagent_resource.description,
                "name": softwareagent_resource.name,
                "softwareSourceCode": {
                    "@type": softwareagent_resource.softwareSourceCode.type,
                    "codeRepository": softwareSourceCode.codeRepository,
                    "programmingLanguage": softwareSourceCode.programmingLanguage,
                    "version": activity_metadata["software_version"],
                    "runtimePlatform": activity_metadata["runtime_platform"],
                },
            }
        ]
        startedAtTime = {
            "@type": "xsd:dateTime",
            "@value": f"{activity_metadata['start_time']}",
        }
        activity_resource = Resource(
            id=activity_metadata["activity_id"],
            type=["Activity"],
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
    config_content,
    new_atlasrelease_hierarchy_path,
    atlasrelease_dict,
    parcellation_found,
    atlas_reference_system_id,
    subject,
):

    spatialReferenceSystem = {
        "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
        "allen_ccfv3_spatial_reference_system",
        "@type": "AtlasSpatialReferenceSystem",
    }

    # average brain model ccfv3
    brainTemplateDataLayer = {
        "@id": "https://bbp.epfl.ch/neurosciencegraph/data/"
        "dca40f99-b494-4d2c-9a2f-c407180138b7",
        "@type": "BrainTemplateDataLayer",
    }

    releaseDate = {
        "@type": "xsd:date",
        "@value": f"{datetime.today().strftime('%Y-%m-%d')}",
    }
    link_to_hierarchy = False
    atlasrelease_resource = []
    if atlasrelease_dict["atlasrelease_choice"] == "atlasrelease_hybridsplit":
        if not new_atlasrelease_hierarchy_path:
            # Atlas release hybrid v2-v3 L2L3 split
            try:
                filters = {"name": "Allen Mouse CCF v2-v3 hybrid l2-l3 split"}
                atlasrelease_resource = forge.search(filters, limit=1)[0]
                atlasrelease_dict["atlas_release"] = atlasrelease_resource
            except Exception as e:
                raise Exception(
                    "Error when searching the BrainAtlasRelease Resource 'Allen "
                    "Mouse CCF v2-v3 hybrid l2-l3 split' in the destination "
                    f"project '{forge._store.bucket}'. {e}"
                )
                exit(1)
        elif "annotation_hybrid_l23split" in parcellation_found:
            description = (
                "This atlas release uses the brain parcellation resulting of the "
                "hybridation between CCFv2 and CCFv3 and integrating the splitting of "
                "layer 2 and layer 3. The average brain template and the ontology is "
                "common across CCFv2 and CCFv3."
            )
            atlasrelease_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name="Allen Mouse CCF v2-v3 hybrid l2-l3 split",
                description=description,
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                subject=subject,
                releaseDate=releaseDate,
            )
            link_to_hierarchy = True
        if not atlasrelease_resource:
            raise Exception(
                "No BrainAtlasRelease 'Allen Mouse CCF v2-v3 hybrid l2-l3 "
                "split' resource found in the destination project "
                f"'{forge._store.bucket}'. Please provide the argument "
                "--new-atlasrelease-hierarchy-path and the right parcellation volume "
                "to first generate and push a new atlas release resource into your "
                "project ."
            )
            exit(1)

    # Atlas Releases realigned split volume
    elif atlasrelease_dict["atlasrelease_choice"] == "atlasrelease_realignedsplit":
        if not new_atlasrelease_hierarchy_path:
            try:
                filters = {"name": "Allen Mouse CCF v2-v3 realigned l2-l3 split"}
                atlasrelease_resource = forge.search(filters, limit=1)[0]
                atlasrelease_dict["atlas_release"] = atlasrelease_resource
            except Exception as e:
                raise Exception(
                    "Error when searching the BrainAtlasRelease Resource 'Allen "
                    "Mouse CCF v2-v3 realigned l2-l3 split' in the destination "
                    f"project '{forge._store.bucket}'. {e}"
                )
                exit(1)
        elif "annotation_realigned_l23split" in parcellation_found:
            description = (
                "This atlas release uses the brain parcellation resulting of the "
                "realignment of CCFv2 over CCFv3 and integrating the splitting of "
                "layer 2 and layer 3. The average brain template and the ontology is "
                "common across CCFv2 and CCFv3."
            )
            atlasrelease_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name="Allen Mouse CCF v2-v3 realigned l2-l3 split",
                description=description,
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                subject=subject,
                releaseDate=releaseDate,
            )
            link_to_hierarchy = True
        if not atlasrelease_resource:
            raise Exception(
                "No BrainAtlasRelease 'Allen Mouse CCF v2-v3 realigned l2-l3 "
                "split' resource found in the destination project "
                f"'{forge._store.bucket}'. Please provide the argument "
                "--new-atlasrelease-hierarchy-path and the right parcellation volume "
                "to first generate and push a new atlas release resource into your "
                "project."
            )
            exit(1)

    # Atlas Releases ccfv3 layer23 split volume
    elif atlasrelease_dict["atlasrelease_choice"] == "atlasrelease_ccfv3split":
        if not new_atlasrelease_hierarchy_path:
            try:
                filters = {"name": "Allen Mouse CCF v3 l2-l3 split"}
                atlasrelease_resource = forge.search(filters, limit=1)[0]
                atlasrelease_dict["atlas_release"] = atlasrelease_resource
            except Exception as e:
                raise Exception(
                    "Error when searching the BrainAtlasRelease Resource 'Allen Mouse "
                    "CCF v3 l2-l3 split' in the destination project "
                    f"'{forge._store.bucket}'. {e}"
                )
                exit(1)
        elif "annotation_ccfv3_l23split" in parcellation_found:
            description = (
                "This atlas release uses the brain parcellation of CCFv3 (2017) with "
                "the isocortex layer 2 and 3 split. The average brain template and the "
                "ontology is common across CCFv2 and CCFv3."
            )
            atlasrelease_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name="Allen Mouse CCF v3 l2-l3 split",
                description=description,
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                subject=subject,
                releaseDate=releaseDate,
            )
            link_to_hierarchy = True
        if not atlasrelease_resource:
            raise Exception(
                "No BrainAtlasRelease 'Allen Mouse CCF v3 l2-l3 split  resource found "
                f"in the destination project '{forge._store.bucket}'. Please provide "
                "the argument --new-atlasrelease-hierarchy-path and the right "
                "parcellation volume to first generate and push a new atlas release "
                "resource into your project."
            )
            exit(1)

    # Old Atlas Releases ccfv2 and ccfv3
    elif atlasrelease_dict["atlasrelease_choice"] == "atlasrelease_ccfv2v3":
        try:
            filters = {"name": "Allen Mouse CCF v2"}
            atlasreleasev2_resource = forge.search(filters, limit=1)[0]
            filters = {"name": "Allen Mouse CCF v3"}
            atlasreleasev3_resource = forge.search(filters, limit=1)[0]
            atlasrelease_dict["atlas_release"] = [
                atlasreleasev2_resource,
                atlasreleasev3_resource,
            ]
            atlasrelease_dict["create_new"] = False
        except Exception as e:
            raise Exception(
                "Error when searching the BrainAtlasRelease Resources 'Allen "
                "Mouse CCF v2' and 'Allen Mouse CCF v3'in the destination "
                f"project '{forge._store.bucket}'. {e}"
            )
            exit(1)
        if not atlasreleasev2_resource or not atlasreleasev3_resource:
            # L.info(
            #     "No BrainAtlasRelease 'Allen Mouse CCF v2' and 'Allen "
            #     "Mouse CCF v3' resources found in the destination project "
            #     f"'{forge._store.bucket}'. They will therefore be created."
            # )
            description_ccfv2 = (
                "This atlas release uses the brain parcellation of CCFv2 (2011). The "
                "average brain template and the ontology is common across CCFv2 and "
                "CCFv3."
            )
            name_ccfv2 = "Allen Mouse CCF v2"
            parcellationOntology = {
                "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
                "@type": ["Entity", "Ontology", "ParcellationOntology"],
            }
            parcellationVolume = {
                "@id": "https://bbp.epfl.ch/neurosciencegraph/data/ "
                "7b4b36ad-911c-4758-8686-2bf7943e10fb",
                "@type": [
                    "Dataset",
                    "VolumetricDataLayer",
                    "BrainParcellationDataLayer",
                ],
            }

            atlasreleasev2_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name=name_ccfv2,
                description=description_ccfv2,
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                subject=subject,
                parcellationOntology=parcellationOntology,
                parcellationVolume=parcellationVolume,
                releaseDate=releaseDate,
            )

            atlasreleasev3_resource = Resource(
                id=forge.format("identifier", "brainatlasrelease", str(uuid4())),
                type=["AtlasRelease", "BrainAtlasRelease"],
                name=name_ccfv2.replace("v2", "v3"),
                description=description_ccfv2.replace("CCFv2 (2011)", "CCFv3 (2017)"),
                brainTemplateDataLayer=brainTemplateDataLayer,
                spatialReferenceSystem=spatialReferenceSystem,
                subject=subject,
                parcellationOntology=parcellationOntology,
                parcellationVolume=parcellationVolume,
                releaseDate=releaseDate,
            )
            atlasrelease_dict["atlas_release"] = [
                atlasreleasev2_resource,
                atlasreleasev3_resource,
            ]
            atlasrelease_dict["create_new"] = True

    # Link the new atlas release to the hierarchy file
    if new_atlasrelease_hierarchy_path and link_to_hierarchy:
        if not atlasrelease_dict["hierarchy"]:
            try:
                if os.path.samefile(
                    new_atlasrelease_hierarchy_path,
                    config_content["HierarchyJson"]["hierarchy_l23split"],
                ):
                    pass
                else:
                    raise Exception(
                        "Error: The atlas regions hierarchy file provided does not "
                        "correspond to 'hierarchy_l23split' from the dataset "
                        "configuration file"
                    )
                    exit(1)
            except FileNotFoundError as error:
                raise FileNotFoundError(f"Error: {error}")
                exit(1)

            description = (
                "AIBS Mouse CCF Atlas regions hierarchy tree file including the split "
                "of layer 2 and layer 3"
            )
            # Original AIBS hierarchy file
            # "@type": ["Entity", "Ontology"],
            derivation = {
                "@type": "Derivation",
                "entity": {
                    "@id": "http://bbp.epfl.ch/neurosciencegraph/ontologies/mba",
                    "@type": "Entity",
                },
            }
            file_extension = os.path.splitext(
                os.path.basename(new_atlasrelease_hierarchy_path)
            )[1][1:]

            content_type = f"application/{file_extension}"
            distribution_file = forge.attach(
                new_atlasrelease_hierarchy_path, content_type
            )

            hierarchy_resource = Resource(
                id=forge.format("identifier", "parcellationontology", str(uuid4())),
                type=["Entity", "Ontology", "ParcellationOntology"],
                name="AIBS Mouse CCF Atlas parcellation ontology L2L3 split",
                distribution=distribution_file,
                description=description,
                derivation=derivation,
                subject=subject,
            )

            hierarchy_resource.label = hierarchy_resource.name
            atlasrelease_resource.parcellationOntology = {
                "@id": hierarchy_resource.id,
                "@type": ["Entity", "ParcellationOntology", "Ontology"],
            }
            atlasrelease_dict["atlas_release"] = atlasrelease_resource
            atlasrelease_dict["hierarchy"] = hierarchy_resource
        else:
            atlasrelease_resource.parcellationOntology = {
                "@id": atlasrelease_dict["hierarchy"].id,
                "@type": ["Entity", "ParcellationOntology", "Ontology"],
            }
            atlasrelease_dict["atlas_release"] = atlasrelease_resource

    return atlasrelease_dict
