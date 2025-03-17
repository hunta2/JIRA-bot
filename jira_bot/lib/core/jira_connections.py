from urllib.parse import urljoin
from dataclasses import dataclass
from typing import List, Optional, Union
import datetime
import re
import dateutil.parser
from jira import JIRA
import jira.resources as jira_resources
from loguru import logger

from jira_bot.lib.tools.constants import (
    CUSTOM_FIELD_MAPPING,
    EPIC_ISSUE_TYPE,
    ISSUE_TYPE_FIELD,
    PROJECT_FIELD,
    RESOLUTION_FIELD,
    UNRESOLVED_RESOLUTION,
)


@dataclass
class JiraEntity:
    """Base class representing a Jira Entity."""

    issue: jira_resources.Issue

    def _get_custom_field_value(self, field_name: str, nested_field: Optional[str] = None) -> Optional[str]:
        """Extract the value for a custom field from a Jira issue given the logical field name."""
        custom_field_id = CUSTOM_FIELD_MAPPING[field_name]
        custom_field = self.issue.raw.get("fields", {}).get(f"customfield_{custom_field_id}", {})
        if nested_field:
            return custom_field.get(nested_field) if custom_field else None
        if isinstance(custom_field, (str, float, int)):
            return custom_field
        return custom_field.get("value") if custom_field else None

    def _extract_from_description(self, field_name: str) -> Optional[str]:
        """Extract a specific field value from the description, can be any column from the description table."""
        if not self.description:
            logger.warning(f"No description found for issue {self.issue.key}")
            return None

        lines = [line.strip() for line in self.description.splitlines() if line.strip()]
        column_names = []
        data_rows = []

        for line in lines:
            if line.startswith("||"):
                columns = [col.strip() for col in re.split(r"\|\|+", line) if col.strip()]
                column_names.extend(columns)
            elif line.startswith("|"):
                row_data = [cell.strip() for cell in re.split(r"\|+", line) if cell.strip()]
                data_rows.extend(row_data)

        if field_name in column_names:
            index = column_names.index(field_name)
            return data_rows[index] if index < len(data_rows) else None
        return None

    @property
    def description(self) -> Optional[str]:
        """Get the description."""
        return self.issue.fields.description

    @property
    def summary(self) -> str:
        """Summary associated with the Jira issue."""
        return self.issue.fields.summary

    @property
    def created(self) -> datetime.date:
        """Date Jira issue was originally created."""
        creation_timestamp = self.issue.fields.created
        return dateutil.parser.parse(creation_timestamp).date()

    @property
    def updated(self) -> datetime.date:
        """Date Jira issue was last updated."""
        update_timestamp = self.issue.fields.updated
        return dateutil.parser.parse(update_timestamp).date()

    @property
    def labels(self) -> List[str]:
        """Get the labels."""
        return self.issue.fields.labels

    @property
    def status_name(self) -> Optional[str]:
        """Get the status name."""
        return self.issue.fields.status.name if self.issue.fields.status else None

    @property
    def status_id(self) -> Optional[str]:
        """Get the status id."""
        return self.issue.fields.status.id if self.issue.fields.status else None

    @property
    def status_category(self) -> Optional[str]:
        """Get the status category."""
        return (
            self.issue.fields.status.statusCategory.name
            if self.issue.fields.status and self.issue.fields.status.statusCategory
            else None
        )

    @property
    def comments_count(self) -> int:
        """Get the total number of comments."""
        return self.issue.fields.comment.total if self.issue.fields.comment else 0

    @property
    def due_date(self) -> Optional[str]:
        """Get the due date."""
        return self.issue.fields.duedate

    @property
    def last_viewed(self) -> Optional[str]:
        """Get the last viewed date."""
        return self.issue.fields.lastViewed

    @property
    def watch_count(self) -> int:
        """Get the watch count."""
        return self.issue.fields.watches.watchCount if self.issue.fields.watches else 0

    @property
    def creator_email(self) -> Optional[str]:
        """Get the creator's email."""
        creator_field = self.issue.raw.get("fields", {}).get("creator", {})
        return creator_field.get("emailAddress") if creator_field else None

    @property
    def assignee_email(self) -> Optional[str]:
        """Get the assignee's email."""
        creator_field = self.issue.raw.get("fields", {}).get("assignee", {})
        return creator_field.get("emailAddress") if creator_field else None


@dataclass
class JiraEpic(JiraEntity):
    """Class representing a Jira Epic."""

    @property
    def epic_id(self) -> str:
        """Get the id from the epic."""
        return self.issue.id  # The ID of the JIRA epic

    @property
    def epic_key(self) -> str:
        """Key for Jira issue."""
        return self.issue.key

    @property
    def protocol_uuid(self) -> str:
        """Get the UUID from the epic."""
        return self._extract_from_description("uuid")

    @property
    def last_updated(self) -> Optional[datetime.date]:
        """Get the last updated timestamp from the description."""
        last_updated_str = self._extract_from_description("last_updated")
        return dateutil.parser.parse(last_updated_str) if last_updated_str else None

    @property
    def protocol_id(self) -> Optional[str]:
        """Extract the Protocol ID from custom fields."""
        return self._get_custom_field_value("Protocol ID")

    @property
    def epic_name(self) -> Optional[str]:
        """Get the epic name."""
        return self._get_custom_field_value("Epic Name")

    @property
    def requestor_email(self) -> Optional[str]:
        """Get the requestor's email."""
        return self._get_custom_field_value("Requestor", "emailAddress")

    @property
    def trial_engineer_email(self) -> Optional[str]:
        """Get the trial engineer's email."""
        return self._get_custom_field_value("Trial Engineer", "emailAddress")

    @property
    def protocol_sheet(self) -> Optional[str]:
        """Get the protocol sheet."""
        return self._get_custom_field_value("Protocol Sheet")

    @property
    def executed_trials(self) -> Optional[str]:
        """Get the number of executed_trials."""
        return self._get_custom_field_value("Executed Trials")

    @property
    def forcasted_costs(self) -> Optional[str]:
        """Get the forcasted costs."""
        return self._get_custom_field_value("Forcasted Costs")

    @property
    def country(self) -> Optional[str]:
        """Get the Country."""
        return self._get_custom_field_value("Country")

    @property
    def crop(self) -> Optional[str]:
        """Get the crop."""
        return self._get_custom_field_value("Crop")

    @property
    def year_of_harvest(self) -> Optional[int]:
        """Get the year of harvest."""
        return self._get_custom_field_value("Year of Harvest", "value")

    @property
    def business_case(self) -> Optional[str]:
        """Get the business case."""
        return self._get_custom_field_value("Business Case")

    @property
    def budget(self) -> Optional[float]:
        """Get the budget."""
        return self._get_custom_field_value("Budget")

    @property
    def paid_costs(self) -> Optional[float]:
        """Get the paid costs."""
        return self._get_custom_field_value("Paid Costs")

    @property
    def planned_trials(self) -> Optional[float]:
        """Get the planned number of trials."""
        return self._get_custom_field_value("Planned Trials")

    @property
    def sponsor(self) -> Optional[str]:
        """Get the sponsor."""
        return self._get_custom_field_value("Sponsor")

    @property
    def cost_sheet(self) -> Optional[str]:
        """Get the cost sheet."""
        return self._get_custom_field_value("Cost Sheet")

    @property
    def trial_type(self) -> Optional[str]:
        """Get the trial type."""
        return self._get_custom_field_value("Trial Type", "value")

    @property
    def trial_objective(self) -> Optional[str]:
        """Get the trial objective."""
        return self._get_custom_field_value("Trial Objective", "value")

    @property
    def url_field(self) -> str:
        """Get the URL field for the epic."""
        return f"https://jira.digital-farming.com/browse/{self.epic_key}"

    # Add the missing components property
    @property
    def components(self) -> Optional[str]:
        """Get the components."""
        return self.issue.fields.components


@dataclass
class JiraIssue(JiraEntity):
    """Class representing a Jira Issue linked to an Epic."""

    @property
    def issue_id(self) -> str:
        """Get the id from the issue."""
        return self.issue.id  # The ID of the JIRA issue

    @property
    def issue_key(self) -> str:
        """Key for Jira issue."""
        return self.issue.key

    @property
    def trial_id(self) -> Optional[str]:
        """Get the trial ID from the issue."""
        return self._get_custom_field_value("Trial-ID")

    @property
    def trial_uuid(self) -> str:
        """Get the UUID from the issue."""
        return self._extract_from_description("uuid")

    @property
    def last_updated(self) -> Optional[datetime.date]:
        """Get the last updated timestamp from the description."""
        last_updated_str = self._extract_from_description("last_updated")
        return dateutil.parser.parse(last_updated_str) if last_updated_str else None

    @property
    def epic_link(self) -> Optional[str]:
        """Get the epic link."""
        return self._get_custom_field_value("Epic Link")

    @property
    def requestor_email(self) -> Optional[str]:
        """Get the requestor's email."""
        return self._get_custom_field_value("Requestor", "emailAddress")

    @property
    def trial_engineer_email(self) -> Optional[str]:
        """Get the trial engineer's email."""
        return self._get_custom_field_value("Trial Engineer", "emailAddress")

    @property
    def subtask_ids(self) -> Optional[List[str]]:
        """Get the subtask ids."""
        return [subtask.id for subtask in self.issue.fields.subtasks]

    @property
    def subtask_keys(self) -> Optional[List[str]]:
        """Get the subtask keys."""
        return [subtask.key for subtask in self.issue.fields.subtasks]


@dataclass
class JiraSubTask(JiraEntity):
    """Class representing a Jira SubTask."""

    @property
    def subtask_id(self) -> str:
        """Get the id from the issue."""
        return self.issue.id  # The ID of the JIRA issue

    @property
    def subtask_key(self) -> str:
        """Key for Jira issue."""
        return self.issue.key

    @property
    def file_uuid(self) -> str:
        """Get the UUID from the issue."""
        return self._extract_from_description("file_uuid")

    @property
    def trial_id(self) -> Optional[str]:
        """Get the trial ID from the issue."""
        return self._get_custom_field_value("Trial-ID")

    @property
    def trial_engineer_email(self) -> Optional[str]:
        """Get the Trial Engineer email from the issue."""
        return self._get_custom_field_value("Trial Engineer")
    
    @property
    def parent_issue(self) -> Optional[str]:
        """Get the issue link."""
        return self.issue.fields.parent.key


@dataclass
class SearchFilter:
    field_name: str
    field_values: Union[str, List[Union[str, None]], None]

    def to_jql(self) -> Optional[str]:
        """Generate a JQL string to apply the filter in a Jira issues query.

        :return: JQL string (if filter is active) or None (if filter is inactive).
        """
        if isinstance(self.field_values, str):
            return self._single_value_filter_jql(self.field_values)
        elif isinstance(self.field_values, list):
            return self._multi_value_filter_jql(self.field_values)
        elif self.field_values is None:
            return None
        else:
            raise ValueError(
                f"Filter values {self.field_values} are not valid: should be a single str, a list of [str, None] or None."
            )

    def _single_value_filter_jql(self, value: str) -> str:
        """Generate the JQL for a filter with a single accepted value.

        :param value: Accepted value for the filter.
        :return: Partial JQL query string for a single field.
        """
        return f'"{self.field_name}"="{value}"'

    def _multi_value_filter_jql(self, values: List[Union[str, None]]) -> str:
        """Generate the JQL for a filter with multiple accepted values.

        :param values: Accepted values for the filter.
        :return: Partial JQL query string for a single field.
        """
        if values == [None]:
            return f'"{self.field_name}" is EMPTY'

        include_empty_values = None in values
        clean_values = [value for value in values if value is not None]

        field_values = ", ".join([f'"{value}"' for value in clean_values])
        filter_jql = f'"{self.field_name}" in ({field_values})'

        return self._include_empty_values(filter_jql) if include_empty_values else filter_jql

    def _include_empty_values(self, filter_jql: str) -> str:
        """Add parenthesis & an extra clause to partial JQL query so that it includes EMPTY values for the filtered field.

        :param filter_jql: Partial JQL query string for a single field which includes only specific field values.
        :return: Partial JQL query string for a single field, modified to include EMPTY values.
        """
        return f'({filter_jql} OR "{self.field_name}" is EMPTY)'

    @property
    def is_active(self) -> bool:
        """Indicates whether a filter contains filter values to be applied."""
        return self.to_jql() is not None


@dataclass
class TriasJira:
    server_url: str
    token: str
    project_id: int

    def __post_init__(self):
        self.jira_connection = self.jira_connect()

    def jira_connect(self):
        options = {
            "server": self.server_url,
            "verify": False,  # Adjust as necessary for SSL verification
        }
        return JIRA(options=options, token_auth=self.token)
    
    def epic_jql_query(self) -> str:
        """Build the JQL query for retrieving unresolved epics.
        :return: A JQL query string.
        """
        project_filter = SearchFilter(field_name=PROJECT_FIELD, field_values=str(self.project_id))
        issue_type_filter = SearchFilter(field_name=ISSUE_TYPE_FIELD, field_values=EPIC_ISSUE_TYPE)
        resolution_filter = SearchFilter(field_name=RESOLUTION_FIELD, field_values=UNRESOLVED_RESOLUTION)

        active_filters = filter(
            lambda x: x.is_active,
            [project_filter, issue_type_filter, resolution_filter],
        )
        return " AND ".join(filter(None, [f.to_jql() for f in active_filters]))

    def issue_jql_query(self) -> str:
        """Build the JQL query for retrieving all issues excluding subtasks.
        :return: A JQL query string.
        """
        project_filter = SearchFilter(field_name=PROJECT_FIELD, field_values=str(self.project_id))
        resolution_filter = SearchFilter(field_name=RESOLUTION_FIELD, field_values=UNRESOLVED_RESOLUTION)

        active_filters = filter(
            lambda x: x.is_active,
            [project_filter, resolution_filter],
        )
        jql_query = " AND ".join(filter(None, [f.to_jql() for f in active_filters]))

        # Add the condition to exclude subtasks
        jql_query += ' AND issuetype != "Sub-task" AND issuetype != "Epic"'

        return jql_query


@dataclass
class TriasEpics:
    """Class for managing TRIAS epics in JIRA."""

    trias_jira: TriasJira

    def get_epics(self, jql_query: str = None) -> List[JiraEpic]:
        """Get all unresolved epics from the JIRA project.
        :return: A list of JiraEpic instances representing the unresolved epics.
        """
        if not jql_query:
            jql_query = self.trias_jira.epic_jql_query()
        start_at = 0
        max_results = 50
        all_epics = []

        try:
            while True:
                epics = self.trias_jira.jira_connection.search_issues(
                    jql_query, startAt=start_at, maxResults=max_results
                )
                if not epics:
                    break
                all_epics.extend(epics)
                start_at += max_results

            return [self.map_to_jira_epic(epic) for epic in all_epics]
        except Exception as e:
            logger.error(f"Failed to query for epics: {e}")
            raise

    def get_epic_by_key(self, epic_key: str) -> JiraEpic:
        """Get a single epic by its key.
        :param epic_key: The key of the epic to query.
        :return: A JiraEpic instance representing the epic.
        """
        try:
            epic = self.trias_jira.jira_connection.issue(epic_key)
            return self.map_to_jira_epic(epic)
        except Exception as e:
            logger.error(f"Failed to get epic by key {epic_key}: {e}")
            raise

    def map_to_jira_epic(self, epic) -> JiraEpic:
        """Map a JIRA issue to a JiraEpic instance.
        :param epic: The JIRA epic issue to map.
        :return: A JiraEpic instance.
        """
        return JiraEpic(issue=epic)


@dataclass
class TriasIssues:
    """Class for managing TRIAS issues in JIRA."""

    trias_jira: TriasJira

    def get_all_issues(self) -> List[JiraIssue]:
        """Get all issues from Jira.
        :return: A list of JiraIssue instances representing all issues.
        """
        jql_query = self.trias_jira.issue_jql_query()
        try:
            issues = self.trias_jira.jira_connection.search_issues(jql_query)
            return [self.map_to_jira_issue(issue) for issue in issues]
        except Exception as e:
            logger.error(f"Failed to query for get all issues: {e}")
            raise

    def get_issues_for_epic(self, epic_key: str) -> List[JiraIssue]:
        """Get all issues linked to a specific epic.
        :param epic_key: The key of the epic to query.
        :return: A list of JiraIssue instances representing the issues linked to the epic.
        """
        jql_query = f'"Epic Link" = "{epic_key}"'
        try:
            issues = self.trias_jira.jira_connection.search_issues(jql_query)
            return [self.map_to_jira_issue(issue) for issue in issues]
        except Exception as e:
            logger.error(f"Failed to query for issues linked to epic {epic_key}: {e}")
            raise

    def get_issue_by_key(self, issue_key: str) -> JiraIssue:
        """Get a single issue by its key.
        :param issue_key: The key of the issue to query.
        :return: A JiraIssue instance representing the issue.
        """
        try:
            issue = self.trias_jira.jira_connection.issue(issue_key)
            return self.map_to_jira_issue(issue)
        except Exception as e:
            logger.error(f"Failed to get issue by key {issue_key}: {e}")

    def map_to_jira_issue(self, issue: jira_resources.Issue) -> JiraIssue:
        """Map a JIRA issue to a JiraIssue instance.
        :param issue: The JIRA issue to map.
        :return: A JiraIssue instance.
        """
        return JiraIssue(issue=issue)


@dataclass
class TriasSubTasks:
    """Class for managing TRIAS subtasks in JIRA."""

    trias_jira: TriasJira

    def get_subtasks_for_issue(self, issue_key: str) -> List[JiraIssue]:
        """Get all subtasks linked to a specific issue.
        :param issue_key: The key of the issue to query.
        :return: A list of JiraIssue instances representing the subtasks linked to the issue.
        """
        jql_query = f'parent = "{issue_key}"'
        try:
            issues = self.trias_jira.jira_connection.search_issues(jql_query)
            return [self.map_to_jira_issue(issue) for issue in issues]
        except Exception as e:
            logger.error(f"Failed to query for subtasks linked to issue {issue_key}: {e}")
            raise

    def get_issue_by_key(self, issue_key: str) -> JiraIssue:
        """Get a single issue by its key.
        :param issue_key: The key of the issue to query.
        :return: A JiraIssue instance representing the issue.
        """
        try:
            issue = self.trias_jira.jira_connection.issue(issue_key)
            return self.map_to_jira_issue(issue)
        except Exception as e:
            logger.error(f"Failed to get issue by key {issue_key}: {e}")

    def map_to_jira_issue(self, issue) -> JiraSubTask:
        """Map a JIRA issue to a JiraIssue instance.
        :param issue: The JIRA issue to map.
        :return: A JiraIssue instance.
        """
        return JiraSubTask(issue=issue)