from prefect import task, get_run_logger, get_client
from prefect.runtime import flow_run
import sqlalchemy as sa
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jira_bot.lib.core.jira_connections import (
        TriasJira,
        TriasEpics,
        TriasIssues,
        JiraEpic,
        JiraSubTask,
        TriasSubTasks,
    )


@task(name="rename-flow-run")
async def rename_flow_run(new_name: str):
    flow_run_id = flow_run.get_id()

    async with get_client() as client:
        await client.update_flow_run(flow_run_id, name=new_name)

@task(name="initialize-jira_bot-flow")
def initialize(payload, logger):
    from jira_bot.lib.core.event_handler import EventHandler

    enable_loguru_support()

    event_handler = EventHandler()
    logger.info(f"payload received: {payload}")
    return event_handler.handle(payload)


def enable_loguru_support():
    """Redirect Loguru logging messages to the Prefect run logger.

    This function should be called from within a Prefect task or flow before calling any module that uses Loguru.
    This function can be safely called multiple times.

    Example Usage:
    from prefect import flow
    from loguru import logger
    from prefect_utils import enable_loguru_support  # import this function in your flow from your module

    @flow()
    def myflow():
        logger.info("This is hidden from the Prefect UI")
        enable_loguru_support()
        logger.info("This shows up in the Prefect UI")
    """
    from loguru import (
        logger,
    )  # Import here for distributed execution because Loguru cannot be pickled.

    run_logger = get_run_logger()
    logger.remove()
    log_format = "{name}:{function}:{line} - {message}"

    logger.add(
        run_logger.warning,
        filter=lambda record: record["level"].name == "WARNING",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.error,
        filter=lambda record: record["level"].name == "ERROR",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.critical,
        filter=lambda record: record["level"].name == "CRITICAL",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.info,
        filter=lambda record: record["level"].name not in ["DEBUG", "WARNING", "ERROR", "CRITICAL"],
        level="TRACE",
        format=log_format,
    )


@task(name="get-aws-region")
def get_current_region():
    import os
    import boto3
    import botocore

    try:
        # Use botocore to get the current AWS region
        session = boto3.session.Session()
        region = session.region_name
        if region is None:
            # If the region is still None, try to fetch it from EC2 metadata
            ec2_client = boto3.client("ec2")
            region = ec2_client.describe_regions()["Regions"][0]["RegionName"]
        return region
    except botocore.exceptions.NoRegionError:
        return os.environ["AWS_REGION"]


@task(name="get-aws-credentals-from-blocks")
def get_aws_credentials(aws_region: str, mode: str = "cluster"):
    """Get AWS credentials from the Prefect aws-credentials block or assume a role if available.

    Args:
        logger (Logger): The logger to use for logging messages.

    Returns:
        boto3.Session: a session object
    """
    import boto3
    from prefect_aws import AwsCredentials

    enable_loguru_support()

    if mode == "cluster":
        aws_credentials_block = AwsCredentials(region_name=aws_region)
        session = boto3.Session(
            aws_access_key_id=aws_credentials_block.aws_access_key_id,
            aws_secret_access_key=aws_credentials_block.aws_secret_access_key,
            aws_session_token=aws_credentials_block.aws_session_token,
        )
    elif mode == "local":
        session = boto3.Session(profile_name="xrnd-modeling-dev")
    else:
        raise ValueError("Invalid mode. Must be either 'cluster' or 'local'.")
    return session


@task(name="initialize-trias-jira")
def initialize_trias_jira(server_url: str, token: str, project_id: int) -> "TriasJira":
    from jira_bot.lib.core.jira_connections import TriasJira

    return TriasJira(server_url=server_url, token=token, project_id=project_id)

@task(name="get-trias-procotol-filter")
def get_trias_protocol_filter(trias_jira: "TriasJira", filter_id: int) -> dict:
    
    return trias_jira.jira_connection.filter(filter_id)

@task(name="initialize-trias-epics")
def initialize_trias_epics(trias_jira: "TriasJira") -> "TriasEpics":
    from jira_bot.lib.core.jira_connections import TriasEpics

    return TriasEpics(trias_jira=trias_jira)


@task(name="initialize-trias-issues")
def initialize_trias_issues(trias_jira: "TriasJira") -> "TriasIssues":
    from jira_bot.lib.core.jira_connections import TriasIssues

    return TriasIssues(trias_jira=trias_jira)


@task(name="initialize_trias_subtasks")
def initialize_trias_subtasks(trias_jira: "TriasJira") -> "TriasSubTasks":
    from jira_bot.lib.core.jira_connections import TriasSubTasks

    return TriasSubTasks(trias_jira=trias_jira)


@task(name="manage-epic")
def manage_epic(
    trias_jira: "TriasJira",
    trias_epics: "TriasEpics",
    trias_issues: "TriasIssues",
    trias_subtasks: "TriasSubTasks",
    engine: sa.engine,
    epic: "JiraEpic",
):
    from jira_bot.lib.core.protocol_manager import ProtocolManager

    protocol_manager = ProtocolManager(trias_jira, trias_epics, trias_issues, trias_subtasks, engine)
    protocol_manager.manage_single_epic(epic)


@task(name="manage-subtasks-for-issue")
def manage_subtasks_for_issue(
    trias_jira: "TriasJira",
    trias_epics: "TriasEpics",
    trias_issues: "TriasIssues",
    trias_subtasks: "TriasSubTasks",
    engine: sa.engine,
    issue_key: str,
):
    from jira_bot.lib.core.protocol_manager import ProtocolManager

    protocol_manager = ProtocolManager(trias_jira, trias_epics, trias_issues, trias_subtasks, engine)
    protocol_manager.manage_subtasks_for_issue(issue_key)
