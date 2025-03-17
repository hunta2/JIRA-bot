import os
from typing import Optional
from prefect import flow, get_run_logger
from jira_bot import tasks
from enum import Enum


class JiraIssueType(str, Enum):
    EPIC = "Epic"
    TRIAL = "Trial"
    SUBTASK = "Subtask"


@flow(name="x-trias-jira-bot-flow", validate_parameters=False)
def flow(jira_issue_key: Optional[str] = None, jira_issue_type: JiraIssueType = JiraIssueType.EPIC):
    """Jira-bot flow"""
    from jira_bot.tasks import enable_loguru_support, get_aws_credentials, get_current_region
    from jira_bot.lib.query.database import get_engine
    from jira_bot.lib.tools.constants import (
        XTRIAS_DB_PARAMS,
        JIRA_SERVER_URL,
        JIRA_TOKEN,
        PROJECT_ID,
        ACTIVE_PROTOCOL_FILTER_ID,
    )

    logger = get_run_logger()
    enable_loguru_support()
    aws_region = get_current_region()
    session = get_aws_credentials(aws_region, os.environ["RUN_ENV"])
    engine = get_engine(os.environ["TRIAS_DB"], session, XTRIAS_DB_PARAMS)
    trias_jira = tasks.initialize_trias_jira(JIRA_SERVER_URL, JIRA_TOKEN, PROJECT_ID)
    trias_epics = tasks.initialize_trias_epics(trias_jira)
    trias_filter = tasks.get_trias_protocol_filter(trias_jira, ACTIVE_PROTOCOL_FILTER_ID)
    logger.info("Trias filter: %s", trias_filter.raw["jql"])
    trias_issues = tasks.initialize_trias_issues(trias_jira)
    trias_subtasks = tasks.initialize_trias_subtasks(trias_jira)
    epics = trias_epics.get_epics(trias_filter.raw["jql"])
    logger.info("Found number of epics: %s", len(epics))
    if jira_issue_type == JiraIssueType.EPIC:
        # Manage epics
        for epic in epics:
            if jira_issue_key and epic.epic_key != jira_issue_key:
                continue

            logger.info(f"Processing epic {epic.epic_key}")
            tasks.rename_flow_run(f"{epic.epic_key}-{epic.epic_name}")
            tasks.manage_epic(trias_jira, trias_epics, trias_issues, trias_subtasks, engine, epic)
            issues = trias_issues.get_issues_for_epic(epic.epic_key)
            for issue in issues:
                tasks.manage_subtasks_for_issue(
                    trias_jira, trias_epics, trias_issues, trias_subtasks, engine, issue.issue_key
                )

            if jira_issue_key:
                break
    elif jira_issue_type == JiraIssueType.TRIAL:
        pass
    elif jira_issue_type == JiraIssueType.SUBTASK:
        tasks.manage_subtasks_for_issue(trias_jira, trias_epics, trias_issues, trias_subtasks, engine, jira_issue_key)
    else:
        logger.error(f"Invalid Jira issue type: {jira_issue_type}")

if __name__ == "__main__":
    env = os.environ.get("ENV", "dev")
    os.environ["ENV"] = env
    os.environ["AWS_REGION"] = "eu-central-1"
    os.environ["RUN_ENV"] = "local"
    os.environ["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION"]
    os.environ["TRIAS_DB"] = "rds!db-3d5b6fc7-6a0f-4109-84d5-a9c2a2068110"

    flow(jira_issue_key="TM-440", jira_issue_type=JiraIssueType.SUBTASK)