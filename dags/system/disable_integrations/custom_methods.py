import base64
from datetime import datetime
from functools import lru_cache
import shutil
import os

import tempfile
import requests

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.email import send_mime_email, build_mime_message

from rail import result, render_template
from rail.lib.artifact import existing_artifact

from system.disable_integrations.config import ORG, TEAM_SLUG, API_BASE_URL, DEFAULT_PR_BODY, \
    GITHUB_USER_TOKEN_VAR_NAME, DISABLE_INACTIVE_DAGS_IGNORE_INTEGRATIONS_VAR_NAME, \
    NEW_BRANCH_BASE_NAME, REPO_NAME, REPO_OWNER, BASE_BRANCH, REQUEST_TIMEOUT


def convert_date_to_str(date_obj:datetime):
    """
    Convert a datetime object to a string in the format YYYY-MM-DDTHH:MM:SS.
    Args:
        date_obj (datetime): The datetime object to convert.
    Returns:
        str: The formatted date string.
    """
    if not date_obj:
        return ""
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S")


def convert_date_str_to_date_time(date_str, date_str_format="%Y-%m-%dT%H:%M:%S", return_format="datetime"):
    """
    Convert a date string in the format YYYY-MM-DDTHH:MM:SS to a datetime object.
    Args:
        date_str (str): The date string to convert.
    Returns:
        datetime: The converted datetime object.
    """
    if not date_str:
        return ""
    if return_format == "date":
        return datetime.strptime(date_str, date_str_format)
    return datetime.strptime(date_str, date_str_format)

def create_new_branch_callable():
    from airflow.models import Variable
    github_user_token = Variable.get(GITHUB_USER_TOKEN_VAR_NAME)
    headers = {"Authorization": f"Bearer {github_user_token}"}
    new_branch_name = f"""{NEW_BRANCH_BASE_NAME}_{result("get_disabled_dag_details")['reg_env']}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"""

    return {
        "branch": new_branch_name,
        "details": create_branch(
            REPO_OWNER, REPO_NAME, BASE_BRANCH, new_branch_name, headers)
    }

def create_branch(repo_owner, repo_name, base_branch, new_branch, header):
    """
    Create a new branch in the specified GitHub repository.
    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        base_branch (str): The base branch to create the new branch from.
        new_branch (str): The name of the new branch to create.
        header (dict): Headers for the request, including authorization.
    Returns:
        dict: The response from the GitHub API after creating the branch.
    Raises:
        AirflowException: If the branch creation fails.
    """
    url = f"{API_BASE_URL}{repo_owner}/{repo_name}/git/refs"

    header["Accept"] = "application/vnd.github.v3+json"
    base_branch_sha_url = f"{API_BASE_URL}{repo_owner}/{repo_name}/git/refs/heads/{base_branch}"
    response = requests.get(base_branch_sha_url,
                            headers=header, timeout=REQUEST_TIMEOUT)
    if response.status_code not in [200, 201]:
        # pylint: disable = line-too-long
        raise AirflowException(
            f"Failure occurred while getting base branch({base_branch}) SHA for repo({repo_name}). Status Code: {response.status_code}. Message: {response.text}")
    base_branch_sha = response.json()["object"]["sha"]

    new_branch_payload = {
        "ref": f"refs/heads/{new_branch}",
        "sha": base_branch_sha
    }

    response = requests.post(url, json=new_branch_payload,
                            headers=header, timeout=REQUEST_TIMEOUT)
    if response.status_code == 201:
        return response.json()
    # pylint: disable = line-too-long
    raise AirflowException(
        f"Failure occurred while creating new branch({new_branch}) in repo({repo_name}). Status Code: {response.status_code}. Message: {response.text}")


def get_file_contents_and_sha(repo_owner, repo_name, file_path, new_branch, header):
    """
    Get the contents and SHA of a file in the specified branch of the GitHub repository.
    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        file_path (str): The path to the file in the repository.
        new_branch (str): The branch name where the file is located.
        header (dict): Headers for the request, including authorization.
    Returns:
        tuple: A tuple containing the file content and its SHA.
    Raises:
        AirflowException: If the file retrieval fails.
    """
    get_file_url = f"{API_BASE_URL}{repo_owner}/{repo_name}/contents/{file_path}?ref={new_branch}"
    response = requests.get(get_file_url, headers=header,
                            timeout=REQUEST_TIMEOUT)
    if response.status_code not in [200, 201]:
        # pylint: disable = line-too-long
        raise AirflowException(
            f"Failure occurred while getting file content for file: {file_path} in new branch({new_branch}) for repo({repo_name}). Status Code: {response.status_code}. Message: {response.text}")

    return (base64.b64decode(response.json()["content"]).decode(), response.json()["sha"])

# pylint: disable = too-many-arguments


def update_file_content_and_encode(repo_owner, repo_name, file_path, branch_name, header, new_content):
    """
    Update the file content and encode it in base64.
    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        file_path (str): The path to the file in the repository.
        branch_name (str): The branch name where the file will be updated.
        header (dict): Headers for the request, including authorization.
        new_content (str): The new content to be added to the file.
    Returns:
        tuple: A tuple containing a boolean indicating whether the file was updated, the encoded content, and the SHA of the current file version.
    Raises:
        AirflowException: If the file update fails.
    """
    current_content, current_sha = get_file_contents_and_sha(
        repo_owner, repo_name, file_path, branch_name, header)
    if "disabled = True" in current_content or "disabled=True" in current_content or "disabled =True" in current_content or\
            "disabled= True" in current_content:
        return (False, "`disabled = True` already present", current_sha)
    return (True, base64.b64encode((current_content + "\n" + new_content).encode()).decode(), current_sha)


# pylint: disable = too-many-arguments
def update_file_via_api(repo_owner, repo_name, file_path, new_branch, header, content, current_sha, commit_message):
    """
    Update the file in the specified branch of the GitHub repository.
    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        file_path (str): The path to the file in the repository.
        new_branch (str): The branch name where the file will be updated.
        header (dict): Headers for the request, including authorization.
        content (str): The new content to be added to the file.
        current_sha (str): The SHA of the current file version.
        commit_message (str): The commit message for the update.
    Raises:
        AirflowException: If the file update fails.
    """
    update_file_url = f"{API_BASE_URL}{repo_owner}/{repo_name}/contents/{file_path}"
    payload = {
        "message": commit_message,
        "content": content,
        "sha": current_sha,
        "branch": new_branch
    }
    response = requests.put(update_file_url, json=payload,
                            headers=header, timeout=REQUEST_TIMEOUT)

    if response.status_code == 200:
        print(
            f"Change Committed: File '{file_path}' updated successfully with `Disabled=True`.")
        return
    # pylint: disable = line-too-long
    raise AirflowException(
        f"Failure occurred while updating file in branch({new_branch}) for repo({repo_name}). Status Code: {response.status_code}. Message: {response.text}")


def get_commit_message(instance_file_path: str):
    """
    Generate a commit message based on the instance file path.
    Args:
        instance_file_path (str): The path of the instance file.
    Returns:
        str: The generated commit message.
    """
    my_path = instance_file_path.split("dags/")[1].split('/')
    company_key = my_path[0]
    integration = my_path[1]
    instance_file_name = my_path[-1]
    return f"For {company_key}'s integration `{integration}`, updated {instance_file_name}"

# pylint: disable = too-many-arguments


def create_pull_request(repo_owner, repo_name, base_branch, new_branch, title, header, body=DEFAULT_PR_BODY):
    """
    Create a pull request for the specified branch in the GitHub repository.
    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        base_branch (str): The base branch to merge into.
        new_branch (str): The new branch to create a pull request for.
        title (str): The title of the pull request.
        header (dict): Headers for the request, including authorization.
        body (str): The body of the pull request. Defaults to a predefined template.
    Returns:
        str: The URL of the created pull request.
    Raises:
        AirflowException: If the pull request creation fails.
    """
    url = f"{API_BASE_URL}{repo_owner}/{repo_name}/pulls"
    payload = {
        "title": title + result('get_disabled_dag_details')['reg_env'],
        "body": body,
        "head": new_branch,
        "base": base_branch
    }

    header["Accept"] = "application/vnd.github.v3+json"

    response = requests.post(
        url, json=payload, headers=header, timeout=REQUEST_TIMEOUT)

    if response.status_code == 201:
        pr_url = response.json()["html_url"]
        print(f"Pull request opened successfully. URL: {pr_url}")
        return pr_url
    for error in response.json()['errors']:
        if error["message"] == f"No commits between main and {new_branch}":
            return "No Pull Request"
    raise AirflowException(
        f"Failed to open pull request for branch({new_branch}) for repo({repo_name}). Status Code: {response.status_code}. Message: {response.text}")


def get_latest_commit_info(owner, repo, path, branch, headers):
    """
    Get the latest commit info for a given path in a GitHub repository.
    Args:
        owner (str): The owner of the repository.
        repo (str): The name of the repository.
        path (str): The path to the file in the repository.
        branch (str): The branch name. (should be the main/master branch)
        headers (dict): Headers for the request, including authorization.
    Returns:
        tuple: A tuple containing the author's name, email, date, commit message and author URL.
    """
    print(f"getting latest commit info for path: {path}")
    commits_url = f"{API_BASE_URL}{owner}/{repo}/commits?path={path}&sha={branch}"
    response = requests.get(commits_url, headers=headers,
                            timeout=REQUEST_TIMEOUT)

    if response.status_code == 200:
        commits = response.json()
        if commits:
            latest_commit = commits[0]
            
            commit_author = latest_commit.get("author") or {}
            return (latest_commit["commit"]["author"]["name"], latest_commit["commit"]["author"]["email"], latest_commit["commit"]["author"]["date"], latest_commit["commit"]["message"], commit_author.get("url", ""))
    raise AirflowException(
        f"Failed to fetch commit history. Status code: {response.status_code}. Message :{response.text}")


def send_standard_response_callable(from_email_addr, to_email_addr, cc_email_addr, caller):
    # pylint: disable=line-too-long
    subject = """Airflow-alert | Disabling un-used dags/integration activity completed. Previous PR#{{result('get_instance_paths_to_change').last_pr_number}} not actioned for {{result('get_disabled_dag_details').reg_env}} | {{ current_time_in_specified_tz('Asia/Kolkata') }}"""
    if caller == "success":
        subject = """Airflow-alert | Disabling un-used dags/integration activity completed for {{result('get_disabled_dag_details').reg_env}} | {{ current_time_in_specified_tz('Asia/Kolkata') }}"""

    if caller == "ignored":
        subject = "Airflow-alert | Disabling un-used dags/integration activity completed for {{result('get_disabled_dag_details').reg_env}} | {{ current_time_in_specified_tz('Asia/Kolkata') }}"

    file_name = "{{result('update_instance_files_to_disable').branch}}"
    _files = []
    if caller == "success":
        _files = [
            (render_template(f"Inactive_dags_list_{file_name}.csv"), result('create_csv'))]
    to_attach = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        def copy_to_staging_dir(source, friendly_name):
            attachment_name = os.path.join(tmp_dir, friendly_name)
            shutil.copyfile(source, attachment_name)
            to_attach.append(attachment_name)

        for file in _files:
            with existing_artifact(file[1]) as artifact:
                copy_to_staging_dir(artifact.local_filename, file[0])

        msg, recipients = build_mime_message(
            mail_from=from_email_addr,
            to=to_email_addr,
            cc=cc_email_addr,
            subject=render_template(subject),
            html_content=result(f"render_email_template_{caller}"),
            files=to_attach
        )

        send_mime_email(e_from=from_email_addr, e_to=recipients, mime_msg=msg)

def get_all_team_members_details_for_integration(header:dict):        
    """
    Fetches details of all team members for a specific integration team from GitHub.
    Args:
        header (tuple): A tuple containing the headers required for the API request.
                        Sent as a tuple as we are using @lru_cache decorator for caching the team information
    Returns:
        dict: A dictionary where the keys are the URLs of the team members and the values are the corresponding team member details.
    Raises:
        AirflowException: If the API request fails, an exception is raised with the status code and error message from the response.
    Note:
        If a new collaborator joins for the airflow-integration repo, The user needs to be added to the GitTeam manually
    """
    url = f"https://api.github.com/orgs/{ORG}/teams/{TEAM_SLUG}/members"
    response = requests.get(url, headers=header)
    
    if response.status_code == 200:
        resp = response.json()
        return {team_member['url']: team_member for team_member in resp}
    else:
        raise AirflowException(f"Failed to fetch team members details for integration team {TEAM_SLUG}. Status code: {response.status_code}. Message: {response.text}")

def validate_if_author_from_integration_team(author_name, author_email, author_url, dice_team_members:dict):
    """
    Validates if the given author is a member of the integration team.
    This function checks if the author's URL exists in the list of team members' details
    retrieved from the integration team (TEAM_SLUG). If the author's URL is found, the function 
    returns True, indicating that the author is part of the integration team. Otherwise, 
    it returns False.
    Args:
        author_name (str): The name of the author.
        author_email (str): The email address of the author.
        author_url (str): The URL associated with the author.
        dice_team_members (dict): A dictionary of team members' details.
    Returns:
        bool: True if the author is a member of the integration team, False otherwise.
    """

    if bool(dice_team_members.get(author_url, False)):
        return True
    return False


@lru_cache(maxsize=32)
def get_last_commit_and_details_on_files() -> dict:
    return result("update_instance_files_to_disable")['last_commit_and_details_on_files']

@lru_cache(maxsize=32)
def get_ignored_list():
    return (Variable.get(DISABLE_INACTIVE_DAGS_IGNORE_INTEGRATIONS_VAR_NAME, deserialize_json=True)).get("ignore_list", [])

def get_integration_path(path):
    print(f"path: {path}")
    path = path.split("instances/")[0]
    return path, path.replace("/opt/airflow/dags/repo/dags/", "").replace("/opt/airflow/dags/", "")
def release_cached_memory():
    """
    This function clears the caches of the following functions:
        - get_ignored_list
        - get_last_commit_and_details_on_files
        - get_all_team_members_details_for_integration
    This ensurers that stale or outdated data is not retained in memory.
    """
    get_ignored_list.cache_clear()
    get_last_commit_and_details_on_files.cache_clear()
    return
