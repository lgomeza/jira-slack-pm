import os
from typing import Optional

import dateutil.parser
import requests
from pydash import get as s_get
from requests.auth import HTTPBasicAuth
from config import Config

from utils import print_json

api_token = Config.JIRA_API_TOKEN
AUTH = HTTPBasicAuth(Config.JIRA_API_EMAIL, api_token)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
PARAMS = {}
BASE_URL = "https://starkmvp.atlassian.net/rest/"


def call_api(uri: str, method="GET", headers=None, auth=AUTH, params=None) -> dict:
    if params is None:
        params = PARAMS
    if headers is None:
        headers = HEADERS
    response = requests.request(
        method, uri, headers=headers, params=params, auth=auth)
    return response.json()


def get_all_users(pprint: bool = False) -> list:
    response = []
    uri = BASE_URL + "api/3/users/search"
    offset = 0
    condition = True
    while condition:
        params = {"startAt": offset}
        users = call_api(uri, params=params)
        if pprint:
            print_json(users)
        if users:
            response += users
            offset += 50
        else:
            condition = False
    return response


def get_all_issues_by_user(account_id: str, pprint=False) -> list:
    uri = BASE_URL + "api/3/search"
    response = []
    offset = 0
    while True:
        query = {"jql": "assignee = {}".format(account_id), "startAt": offset}
        issues = call_api(uri, params=query)
        if pprint:
            print_json(issues)
        if issues.get("issues"):
            response += issues.get("issues")
            offset += 50
        else:
            break
    return response


def get_issues_in_current_week_by_user(account_id: str, pprint=False) -> list:
    uri = BASE_URL + "api/3/search"
    response = []
    offset = 0
    while True:
        query = {
            "jql": "assignee = {} and created >= startOfWeek()".format(account_id),
            "startAt": offset,
        }
        issues = call_api(uri, params=query)
        if pprint:
            print_json(issues)
        if issues.get("issues"):
            response += issues.get("issues")
            offset += 50
        else:
            break
    return response


def get_sp_brute_force(fields: dict, is_custom_field=False) -> Optional[int]:
    for k, v in fields.items():
        if isinstance(v, float) or isinstance(v, int):
            if is_custom_field:
                if "customfield" in k:
                    return v
            else:
                return v
    return


def get_info_from_issue(issue: dict) -> dict:

    project_name = s_get(issue, "fields.project.name")
    if project_name == "Tyba":
        project = s_get(issue, "fields.components")
        if len(project) > 0:
            project_name = project[0]["name"]

    sprint = s_get(issue, "fields.customfield_10021")
    most_recent_sprint_state = None
    most_recent_sprint_name = None
    if sprint and len(sprint) > 0:
        most_recent_sprint_state = sprint[len(sprint) - 1]["state"]
        most_recent_sprint_name = sprint[len(sprint) - 1]["name"]

    tester = s_get(issue, "fields.customfield_10050")
    tester_mail = None
    if tester:
        tester_mail = tester[0]["accountId"]

    return {
        "story_points": get_sp_brute_force(
            issue.get("fields", {}), is_custom_field=True
        ),
        "status": s_get(issue, "fields.status.statusCategory.name"),
        "stage": s_get(issue, "fields.status.name"),
        "priority": s_get(issue, "fields.priority.name"),
        "issue_id": issue.get("id"),
        "issue_name": issue.get("key"),
        "project_name": project_name,
        "issue_summary": s_get(issue, "fields.summary"),
        "creator": s_get(issue, "fields.creator.accountId"),
        "reporter": s_get(issue, "fields.reporter.accountId"),
        "created_at": str(dateutil.parser.parse(s_get(issue, "fields.created"))),
        "updated_at": str(dateutil.parser.parse(s_get(issue, "fields.updated"))),
        "issue_type": s_get(issue, "fields.issuetype.name"),
        "tester": tester_mail,
        "sprint_status": most_recent_sprint_state,
        "sprint_name": most_recent_sprint_name}


def get_all_boards(pprint: bool = False) -> list:
    response = []
    uri = BASE_URL + "agile/1.0/board"
    offset = 0
    condition = True
    while condition:
        params = {"startAt": offset}
        boards = call_api(uri, params=params)
        if pprint:
            print_json(boards)
        if boards.get("values"):
            response += boards.get("values")
            offset += 50
        else:
            condition = False
    return response


def get_all_sprints_by_board(board_id: int, pprint=False) -> list:
    uri = BASE_URL + f"agile/1.0/board/{board_id}/sprint"
    response = []
    offset = 0
    while True:
        params = {"startAt": offset}
        sprints = call_api(uri, params=params)
        if pprint:
            print_json(sprints)
        if sprints.get("values"):
            response += sprints.get("values")
            offset += 50
        else:
            break
    return response
