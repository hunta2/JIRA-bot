import json
import os
import boto3
from awswrangler.secretsmanager import get_secret
from prefect_aws import AwsCredentials

if os.environ.get("RUN_ENV", "local") == "local":
    session = boto3.Session(profile_name="xrnd-modeling-dev")
else:
    aws_credentials_block = AwsCredentials(region_name="eu-central-1")
    session = boto3.Session(
        aws_access_key_id=aws_credentials_block.aws_access_key_id,
        aws_secret_access_key=aws_credentials_block.aws_secret_access_key,
        aws_session_token=aws_credentials_block.aws_session_token,
    )

XTRIAS_DB_PARAMS = {
    "host": "trias-postgres.cvgzlp4fem9o.eu-central-1.rds.amazonaws.com",
    "port": 5432,
    "database": "trias",
}

JIRA_API_SECRETS = json.loads(get_secret(os.environ.get("JIRA_API_SECRETS", "dev/trias-technical-user"), session))
JIRA_TOKEN = JIRA_API_SECRETS["jira_token"]
JIRA_USER = JIRA_API_SECRETS["username"]
ACTIVE_PROTOCOL_FILTER_ID = 33082
BOARD_ID = 2450
PROJECT_ID = 19413
JIRA_SERVER_URL = "https://jira.digital-farming.com"
PROJECT_FIELD = "project"
ISSUE_TYPE_FIELD = "issuetype"
RESOLUTION_FIELD = "resolution"
EPIC_ISSUE_TYPE = "Epic"
UNRESOLVED_RESOLUTION = "Unresolved"
EPIC_NAME_PATTERN = r"^\d{4}-(?!XXX|XQA)\w{3}-\w{2}-\w{5}-\d{2}$"
STATUS_WAITING = "Waiting"
STATUS_WAITING_FOR_DATA = "Waiting for Data"
FIELD_PROTOCOL = "protocol"
FIELD_NAME = "name"
FIELD_TRIAL = "trial"
FIELD_CROP_SEASON_UUID = "crop_season_uuid"
UPLOADED_CROPSEASON_UUID = "cropSeasonUuid"
FIELD_FILE_UUID = "file_uuid"
FIELD_UUID = "uuid"

MZ_COLUMNS = ["uuid", "fieldUuid", "type", "name", "area", "replicate", "treatment", "geom"]

# note planned trials, budget Spent Budget as Paid costs needs to be updated
CUSTOM_FIELD_MAPPING = {
    "Protocol ID": "12491",
    "Requestor": "12492",
    "Trial Engineer": "12493",
    "Protocol Sheet": "12494",
    "Year of Harvest": "12496",
    "Business Case": "12497",
    "Budget": "12498",
    "Paid Costs": "12499",
    "Sponsor": "12500",
    "Cost Sheet": "12501",
    "Due Date": "duedate",
    "Epic Name": "10691",
    "Epic Link": "10690",
    "Planned Trials": "12503",
    "Executed Trials": "12504",
    "Forcasted Costs": "12505",
    "Country": "12495",
    "Crop": "12502",
    "Trial-ID": "12506",
    "Trial Type": "12508",
    "Trial Objective": "12507",
}

EPIC_FIELDS = [
    "protocol_uuid",
    "last_updated",
    "epic_id",
    "epic_key",
    "summary",
    "protocol_id",
    "requestor_email",
    "assignee_email",
    "trial_engineer_email",
    "protocol_sheet",
    "year_of_harvest",
    "business_case",
    "trial_type",
    "trial_objective",
    "budget",
    "paid_costs",
    "planned_trials",
    "executed_trials",
    "forcasted_costs",
    "country",
    "crop",
    "sponsor",
    "cost_sheet",
    "url_field",
    "components",
    "created",
    "updated",
    "last_viewed",
    "watch_count",
    "labels",
    "status_name",
    "status_id",
    "status_category",
    "creator_email",
    "comments_count",
    "due_date",
]

ISSUE_FIELDS = [
    "trial_uuid",
    "trial_id",
    "created",
    "updated",
    "last_viewed",
    "watch_count",
    "labels",
    "status_name",
    "status_id",
    "status_category",
    "summary",
    "creator_email",
    "comments_count",
    "due_date",
    "issue_id",
    "issue_key",
    "trial_engineer_email",
    "last_updated",
    "epic_link",
    "requestor_email",
    "assignee_email",
    "subtask_ids",
    "subtask_keys",
]

EPIC_DESC_COLUMNS = [
    "uuid",
    "date_created",
    "last_updated",
    "country",
    "crop",
    "description",
    "factor_levels",
    "replicates",
    "plot_length",
    "plot_width",
    "min_plot_area",
    "max_plot_area",
    "is_active",
    "name",
    "type_name",
    "target_year",
    "protocol_number",
    "update_timestamp",
    "flow_run_id",
]

TRIAL_DESC_COLUMNS = [
    "uuid",
    "date_created",
    "last_updated",
    "country",
    "name",
    "comment",
    "protocol_uuid",
    "crop_season_uuid",
    "field_uuid",
    "field_zone_uuids",
    "factor_levels",
    "replicates",
    "plot_length",
    "plot_width",
    "min_plot_area",
    "max_plot_area",
    "is_active",
    "trial_number",
    "update_timestamp",
    "flow_run_id",
    "is_abandoned",
]

STATUS_MAPPING = {
    "New Request": "1",
    "In Review": "10549",
    "In Planning": "11920",
    "In Execution": "11921",
    "Waiting for Data": "11922",
    "Waiting for Harvest": "11923",
    "Data Control": "11924",
    "Analysis": "10000",
    "Presenting": "11925",
    "Done": "10322",
    "Canceled": "10526",
}

# all possible workflows, do not need all of them
WORKFLOW_TRANSITIONS = {
    STATUS_MAPPING["New Request"]: {"In Review": 211},  # New Request -> In Review
    STATUS_MAPPING["In Review"]: {"In Planning": 271},  # In Review -> To Be Planned
    STATUS_MAPPING["In Planning"]: {"In Execution": 281},  # To Be Planned -> In Execution
    STATUS_MAPPING["In Execution"]: {
        "Waiting for Data": 351,
        "Waiting for Harvest": 291,
        "Data Control": 371,
    },  # In Execution -> Waiting for Data, Waiting for Harvest, QA/QC
    STATUS_MAPPING["Waiting for Data"]: {
        "Data Control": 311,
        "Waiting for Harvest": 381,
    },  # Waiting for Data -> QA/QC, Waiting for Harvest
    STATUS_MAPPING["Waiting for Harvest"]: {
        "Data Control": 361,
        "Waiting for Data": 301,
    },  # Waiting for Harvest -> QA/QC, Waiting for Data
    STATUS_MAPPING["Data Control"]: {"Analysis": 321},  # QA/QC -> To be Analyzed
    STATUS_MAPPING["Analysis"]: {
        "Ready to be Presented": 331,
        "Back to Data Control": 521,
    },  # Analysis -> Ready to be Presented, Back to Data Control
    STATUS_MAPPING["Presenting"]: {
        "To be Done": 341,
        "Back to Analysis": 531,
    },  # Ready to be Presented -> To be Done, Back to Analysis
}

SUBTASKS_STATUS_MAPPING = {
    "New Request": "1",
    "In Progress": "3",
    "Waiting": "10820",
}

SUBTASKS_WORKFLOW = {
    SUBTASKS_STATUS_MAPPING["New Request"]: {"In Progress": 61},
    SUBTASKS_STATUS_MAPPING["In Progress"]: {"Waiting": 21},
    SUBTASKS_STATUS_MAPPING["Waiting"]: {"Done": 51},
}

EPIC_SCHEMA = {
    "protocol_uuid": "VARCHAR(36)",
    "last_updated": "TIMESTAMP",
    "epic_id": "INTEGER",
    "epic_key": "VARCHAR(255)",
    "summary": "TEXT",
    "protocol_id": "VARCHAR(50)",
    "assignee_email": "VARCHAR(255)",
    "requestor_email": "VARCHAR(255)",
    "trial_engineer_email": "VARCHAR(255)",
    "protocol_sheet": "TEXT",
    "year_of_harvest": "INTEGER",
    "country": "VARCHAR(2)",
    "crop": "VARCHAR(255)",
    "business_case": "TEXT",
    "trial_type": "VARCHAR(255)",
    "trial_objective": "VARCHAR(255)",
    "budget": "NUMERIC(10, 2)",
    "paid_costs": "NUMERIC(10, 2)",
    "forcasted_costs": "NUMERIC(10, 2)",
    "planned_trials": "INTEGER",
    "executed_trials": "INTEGER",
    "sponsor": "VARCHAR(255)",
    "cost_sheet": "TEXT",
    "url_field": "TEXT",
    "components": "TEXT",
    "created": "TIMESTAMP",
    "updated": "TIMESTAMP",
    "last_viewed": "TIMESTAMP",
    "watch_count": "INTEGER",
    "labels": "TEXT",
    "status_name": "VARCHAR(50)",
    "status_id": "VARCHAR(50)",
    "status_category": "VARCHAR(50)",
    "creator_email": "VARCHAR(255)",
    "comments_count": "INTEGER",
    "due_date": "DATE",
}

ISSUE_SCHEMA = {
    "trial_uuid": "VARCHAR(36)",
    "trial_id": "VARCHAR(50)",
    "created": "TIMESTAMP",
    "updated": "TIMESTAMP",
    "last_viewed": "TIMESTAMP",
    "watch_count": "INTEGER",
    "labels": "TEXT[]",
    "status_name": "VARCHAR(50)",
    "status_id": "VARCHAR(50)",
    "status_category": "VARCHAR(50)",
    "summary": "TEXT",
    "creator_email": "VARCHAR(255)",
    "comments_count": "INTEGER",
    "due_date": "DATE",
    "issue_id": "VARCHAR(255)",
    "issue_key": "VARCHAR(255)",
    "trial_engineer_email": "VARCHAR(255)",
    "assignee_email": "VARCHAR(255)",
    "last_updated": "TIMESTAMP",
    "epic_link": "VARCHAR(255)",
    "requestor_email": "VARCHAR(255)",
    "subtask_ids": "TEXT[]",
    "subtask_keys": "TEXT[]",
}
SUBTASK_FIELDS = [
    "parent_issue",
    "subtask_key",
    "file_uuid",
    "trial_id",
    "created",
    "updated",
    "last_viewed",
    "watch_count",
    "labels",
    "status_name",
    "status_category",
    "status_id",
    "summary",
    "creator_email",
    "comments_count",
    "trial_engineer_email",
    "assignee_email",
]

SUBTASK_SCHEMA = {
    "subtask_key": "VARCHAR(255)",
    "file_uuid": "VARCHAR(36)",
    "trial_id": "VARCHAR(50)",
    "created": "timestamp",
    "updated": "timestamp",
    "last_viewed": "timestamp",
    "watch_count": "INTEGER",
    "labels": "text[]",
    "status_name": "VARCHAR(50)",
    "status_category": "VARCHAR(50)",
    "status_id": "VARCHAR(50)",
    "summary": "TEXT",
    "creator_email": "VARCHAR(255)",
    "comments_count": "INTEGER",
    "trial_engineer_email": "VARCHAR(255)",
    "assignee_email": "VARCHAR(255)",
}
