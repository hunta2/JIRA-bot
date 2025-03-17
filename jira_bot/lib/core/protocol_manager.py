"""Main module for managing protocols and associated JIRA tickets."""

from dataclasses import dataclass
from typing import List, Optional, Tuple
import re
import pandas as pd
import sqlalchemy as sa
from loguru import logger

from jira_bot.lib.core.jira_connections import (
    JiraEpic,
    JiraIssue,
    TriasJira,
    TriasEpics,
    TriasIssues,
    JiraSubTask,
    TriasSubTasks,
)

from jira_bot.lib.query.database import (
    get_record_by_name,
    get_record_by_id,
    get_record_by_uuid,
    query_farm_field_names,
)

from jira_bot.lib.tools.constants import (
    CUSTOM_FIELD_MAPPING,
    EPIC_FIELDS,
    EPIC_DESC_COLUMNS,
    TRIAL_DESC_COLUMNS,
    ISSUE_FIELDS,
    EPIC_SCHEMA,
    ISSUE_SCHEMA,
    STATUS_MAPPING,
    WORKFLOW_TRANSITIONS,
    SUBTASKS_WORKFLOW,
    SUBTASKS_STATUS_MAPPING,
    SUBTASK_FIELDS,
    SUBTASK_SCHEMA,
    EPIC_NAME_PATTERN,
    STATUS_WAITING,
    STATUS_WAITING_FOR_DATA,
    FIELD_PROTOCOL,
    FIELD_NAME,
    FIELD_TRIAL,
    FIELD_CROP_SEASON_UUID,
    UPLOADED_CROPSEASON_UUID,
    FIELD_FILE_UUID,
    FIELD_UUID,
)

from jira_bot.lib.tools.helper_functions import (
    create_jira_description,
    Trial,
    MapPlotter,
    compare_fields,
    create_labels,
    search_user,
    create_jira_ticket,
    upsert_record,
)

# Trigger gobbler flow from here
# from prefect import flow
# from gobbler.flows.gobbler_flow import gobbler_flow

# def trigger_gobbler_flow(subtask: JiraSubTask) -> None:
#     """Trigger the gobbler flow when a subtask is marked as 'Done'."""
#     gobbler_flow(uuid=subtask.file_uuid)


@dataclass
class ProtocolManager:
    """Class for managing protocols and associated JIRA tickets."""

    trias_jira: TriasJira
    trias_epics: TriasEpics
    trias_issues: TriasIssues
    trias_subtasks: TriasSubTasks
    engine: sa.engine

    def manage_epics(self) -> None:
        """Fetch all JIRA epics, check against epic DB, and update as necessary."""
        epics = self.trias_epics.get_epics()  # Get all epics from JIRA
        for epic in epics:
            self.manage_single_epic(epic)

    def manage_single_epic(self, epic: JiraEpic) -> None:
        """Manage a single JIRA epic."""
        try:
            if self.is_valid_epic_name(epic.protocol_id):
                protocol_df = get_record_by_name(self.engine, FIELD_PROTOCOL, FIELD_NAME, epic.protocol_id)
                if not protocol_df.empty:
                    self.handle_existing_protocol(epic)
                elif self.check_unseen_protocol_ids(epic.protocol_id, epic.protocol_id):
                    return
                self.manage_trials_for_epic(epic)
        except Exception as e:
            logger.error(f"Error managing epic {epic.epic_key}: {e}")

    def handle_existing_protocol(self, epic: JiraEpic) -> None:
        """Handle existing protocol in the database."""
        epic_df = get_record_by_id(self.engine, "epics", "epic_id", epic.epic_id)
        if not epic_df.empty:
            logger.info(
                f"Epic {epic.epic_key} already exists in the database, will check if protocol has been updated"
            )
            if self.is_protocol_updated(epic):
                self.update_epic_ticket_def("Epic ", epic, " protocol has been updated, will update the Epic")
            if self.epic_changed(epic):
                self.update_epic_ticket_def(
                    "Epic ", epic, " has been altered, will update the ticket.", new_version=True
                )
        else:
            self.update_epic_ticket_def(
                "New epic ", epic, " found, will update ticket with labels and protocol table and add issue links."
            )

    def manage_subtasks_for_issue(self, issue_key: str) -> None:
        """Manage subtasks for a specific issue."""
        issue = self.trias_issues.get_issue_by_key(issue_key)
        cropseason_uuid = get_record_by_name(self.engine, FIELD_TRIAL, FIELD_NAME, issue.trial_id)[
            FIELD_CROP_SEASON_UUID
        ][0]
        uploaded_data = get_record_by_uuid(self.engine, "uploaded_data", UPLOADED_CROPSEASON_UUID, cropseason_uuid)
        if uploaded_data.empty:
            logger.warning(f"No uploaded data found for {issue_key}.")
            return
        subtasks = self.trias_subtasks.get_subtasks_for_issue(issue_key)

        self.create_or_update_subtasks(issue_key, uploaded_data, subtasks)


    def create_or_update_subtasks(
        self, issue_key: str, uploaded_data: pd.DataFrame, subtasks: List[JiraSubTask]
    ) -> None:
        """Create new subtasks in Jira if they don't exist and update existing subtasks."""
        for _, row in uploaded_data.iterrows():
            if row[FIELD_FILE_UUID] not in [subtask.file_uuid for subtask in subtasks] and get_record_by_uuid(
                self.engine, "sub_tasks", FIELD_FILE_UUID, row[FIELD_FILE_UUID]
            ).empty:
                subtask_id = self.create_subtask_in_jira(issue_key, row)
                subtask = self.trias_subtasks.get_issue_by_key(subtask_id)
                self.upsert_subtask(subtask)
            else:
                for subtask in subtasks:
                    if self.subtask_changed(subtask):
                        self.upsert_subtask(subtask)

    def manage_trials_for_epic(self, epic: JiraEpic) -> None:
        """Query trials linked to an epic and create new tickets for any that have been updated or are not already created."""
        try:
            protocol_uuid = get_record_by_name(self.engine, FIELD_PROTOCOL, FIELD_NAME, epic.protocol_id)[
                FIELD_UUID
            ].iloc[0]
        except Exception as e:
            logger.error(f"Failed to get protocol UUID for {epic.protocol_id}: {e}")
            return
        trials = get_record_by_uuid(self.engine, "trial", "protocol_uuid", protocol_uuid)
        existing_issues = self.trias_issues.get_issues_for_epic(epic.epic_key)
        existing_issue_dict = {issue.summary: issue for issue in existing_issues}

        for trial in trials.itertuples(index=False):
            if trial.name in existing_issue_dict:
                self.handle_existing_issue(epic, existing_issue_dict[trial.name], trial)
            else:
                logger.info(f"Creating new issue for trial {trial.name} in epic {epic.epic_key}")
                self.create_new_issue(epic, trial)

    def handle_existing_issue(self, epic: JiraEpic, existing_issue: JiraIssue, trial: Trial) -> None:
        """Handle an existing issue linked to an epic."""
        logger.info(f"Trial {trial.name} already exists in JIRA, checking for updates.")
        if self.is_trial_updated(existing_issue):
            self.update_jira_ticket(epic, existing_issue, trial)
            existing_issue = self.trias_issues.get_issue_by_key(existing_issue.issue_key)
            self.upsert_issue(existing_issue)
        elif self.issue_changed(existing_issue):
            existing_issue = self.trias_issues.get_issue_by_key(existing_issue.issue_key)
            self.upsert_issue(existing_issue)

    def create_new_issue(self, epic: JiraEpic, trial: Trial) -> None:
        """Create a new issue linked to an epic."""
        issue_key = self.create_jira_ticket_with_epic_link(epic, trial)
        logger.info(f"New issue created for trial {trial.name}: {issue_key}")
        self.trias_jira.jira_connection.add_issues_to_epic(epic.epic_key, [issue_key])
        self.attach_images_or_maps(issue_key, trial)
        jira_transition_manager = JiraTransitionManager(self.trias_jira)
        jira_transition_manager.transition_issue(issue_key, STATUS_WAITING_FOR_DATA)
        new_issue = self.trias_issues.get_issue_by_key(issue_key)
        self.upsert_issue(new_issue)

    def is_valid_epic_name(self, epic_name: str) -> bool:
        """Check if the epic name is valid."""
        return re.match(EPIC_NAME_PATTERN, epic_name) is not None

    def update_epic_ticket_def(
        self, logger_f_string_1: str, epic: JiraEpic, logger_f_string_2: str, new_version: bool = False
    ) -> None:
        logger.info(f"{logger_f_string_1}{epic.epic_key}{logger_f_string_2}")
        self.update_epic_ticket(epic)
        epic = self.trias_epics.get_epic_by_key(epic.epic_key)
        self.upsert_epic(epic, new_version)

    def is_protocol_updated(self, epic: JiraEpic) -> bool:
        """Check if the protocol associated with the epic is updated."""
        protocol_result = get_record_by_name(self.engine, FIELD_PROTOCOL, FIELD_NAME, epic.protocol_id)
        if (
            not protocol_result.empty
            and epic.last_updated
            and protocol_result["last_updated"].iloc[0] > epic.last_updated
        ):
            logger.info(f"Found newer protocol for Epic {epic.epic_key}, description will be updated.")
            return True
        return False

    def epic_changed(self, epic: JiraEpic) -> bool:
        """Check if the epic has been updated."""
        epics_db = get_record_by_id(self.engine, "epics", "epic_id", epic.epic_id)
        if not epics_db.empty:
            return compare_fields(epics_db, EPIC_FIELDS, epic, EPIC_SCHEMA)
        logger.warning(f"Epic {epic.epic_key} not found in the database, will generate epic")
        return True

    def check_unseen_protocol_ids(self, epic_name: str, protocol_id: str) -> bool:
        """Check for new/unseen protocol IDs in epic names."""
        # This is a placeholder implementation
        return True

    def update_epic_fields(self, epic: JiraEpic, new_description: str, labels: List[str]) -> None:
        """Update the description and labels of a Jira epic."""
        issue = self.trias_jira.jira_connection.issue(epic.epic_key)
        issue.update(fields={"description": new_description, "labels": labels})

    def update_epic_ticket(self, epic: JiraEpic) -> None:
        """Update the JIRA epic ticket and the epic database."""
        protocol_result = get_record_by_name(self.engine, FIELD_PROTOCOL, FIELD_NAME, epic.protocol_id)
        epic_description = create_jira_description(epic.description, protocol_result, EPIC_DESC_COLUMNS)
        labels = create_labels(epic)
        logger.info(f"Updating epic {epic.epic_id} with new description and labels {labels}.")
        self.update_epic_fields(epic, epic_description, labels)

    def create_jira_ticket_with_epic_link(self, epic: JiraEpic, trial: Trial) -> Optional[str]:
        """Create a Jira ticket linked to an epic."""
        labels = create_labels(epic)
        farm_field_labels = self.add_field_farm_as_labels(trial)
        labels = [label.replace(" ", "-") for label in labels]
        ticket_description = create_jira_description(
            None,
            trial,
            TRIAL_DESC_COLUMNS,
            epic=False,
            farm_name=farm_field_labels[0],
            field_name=farm_field_labels[1],
        )

        assignee_info = search_user(self.trias_jira, epic.assignee_email)
        logger.info(f"Assignee info: {assignee_info}")
        if len(assignee_info) > 0:
            account_id, assignee_name = assignee_info[0], assignee_info[1]
        else:
            account_id, assignee_name = None, None
        
        custom_fields = {
            f"customfield_{CUSTOM_FIELD_MAPPING['Trial-ID']}": trial.name,
            f"customfield_{CUSTOM_FIELD_MAPPING['Trial Engineer']}": {
                "accountId": account_id,
                "name": assignee_name,
            },
        }

        return create_jira_ticket(
            self.trias_jira,
            str(self.trias_jira.project_id),
            trial.name,
            ticket_description,
            "Trial",
            None,
            labels,
            assignee_name,
            custom_fields,
        )

    def update_jira_ticket(self, epic: JiraEpic, issue_exisitng: JiraIssue, trial: Trial) -> None:
        """Update an existing Jira ticket linked to an epic."""
        ticket_description = create_jira_description(issue_exisitng.description, trial, TRIAL_DESC_COLUMNS, epic=False)
        fields = {"description": ticket_description}
        try:
            issue = self.trias_jira.jira_connection.issue(issue_exisitng.issue_key)
            issue.update(fields=fields)
            logger.info(f"Ticket updated for {epic.protocol_id}: {issue.key}")
        except Exception as e:
            logger.error(f"Failed to update ticket for {epic.epic_key}: {e}")

    def is_trial_updated(self, issue_exisitng: JiraIssue) -> bool:
        """Check if the trial has been updated."""
        trial_result = get_record_by_name(self.engine, FIELD_TRIAL, FIELD_NAME, issue_exisitng.summary)
        if not trial_result.empty and trial_result["last_updated"].iloc[0] > issue_exisitng.last_updated:
            logger.info(f"Found newer trial {issue_exisitng.summary}, description will be updated.")
            return True
        return False

    def issue_changed(self, issue: JiraIssue) -> bool:
        """Check if the issue has been updated."""
        issues_db = get_record_by_id(self.engine, "issues", "issue_id", issue.issue_id)
        if not issues_db.empty:
            return compare_fields(issues_db, ISSUE_FIELDS, issue, ISSUE_SCHEMA)
        logger.warning(f"Issue {issue.issue_key} not found in the database, will generate issue")
        return True

    def add_field_farm_as_labels(self, trial: Trial) -> List[str]:
        """Add farm and field labels to the JIRA ticket based on the protocol ID."""
        field_name, farm_uuid = query_farm_field_names(self.engine, trial.field_uuid, "field")
        farm_name = query_farm_field_names(self.engine, farm_uuid, "farm")
        return [farm_name, field_name]

    def create_subtask_in_jira(self, parent_issue_key: str, subtask_data: pd.Series) -> Optional[str]:
        """Create a subtask in Jira."""
        parent_issue = self.trias_jira.jira_connection.issue(parent_issue_key)
        labels = parent_issue.fields.labels
        if parent_issue.fields.assignee:
            assignee = parent_issue.fields.assignee.name
        else:
            assignee = None

        summary = f"{parent_issue.fields.summary}_{subtask_data['type']}"
        description = create_jira_description(
            None,
            subtask_data.to_frame().T,
            columns=subtask_data.index.tolist(),
            epic=False,
        )

        custom_fields = {}

        new_subtask_key = create_jira_ticket(
            self.trias_jira,
            str(self.trias_jira.project_id),
            summary,
            description,
            "Sub-task",
            parent_issue_key,
            labels,
            assignee,
            custom_fields,
        )

        if new_subtask_key:
            jira_transition_manager = JiraTransitionManager(self.trias_jira)
            jira_transition_manager.transition_issue(new_subtask_key, STATUS_WAITING)
        return new_subtask_key

    def subtask_changed(self, subtask: JiraSubTask) -> bool:
        """Check if the subtask has been updated."""
        subtasks_db = get_record_by_uuid(self.engine, "sub_tasks", FIELD_FILE_UUID, subtask.file_uuid)
        if not subtasks_db.empty:
            return compare_fields(subtasks_db, SUBTASK_FIELDS, subtask, SUBTASK_SCHEMA)
        logger.warning(f"Subtask {subtask.subtask_key} not found in the database, will generate subtask")
        return True

    def upsert_issue(self, issue: JiraIssue) -> None:
        """Upsert the issue information into the database."""
        issue_dict = {field: getattr(issue, field) for field in ISSUE_FIELDS}
        upsert_record(self.engine, "issues", issue_dict, issue.issue_id, "issue_id", ISSUE_SCHEMA)

    def upsert_epic(self, epic: JiraEpic, new_version: bool = False) -> None:
        """Upsert the epic information into the database."""
        epic_dict = {field: getattr(epic, field) for field in EPIC_FIELDS}
        upsert_record(self.engine, "epics", epic_dict, epic.epic_id, "epic_id", EPIC_SCHEMA, new_version)

    def upsert_subtask(self, subtask: JiraSubTask) -> None:
        """Upsert the subtask information into the database."""
        subtask_dict = {field: getattr(subtask, field) for field in SUBTASK_FIELDS}
        upsert_record(self.engine, "sub_tasks", subtask_dict, subtask_dict["subtask_key"], "subtask_key", SUBTASK_SCHEMA)

    def attach_images_or_maps(self, ticket_key: str, trial: Trial) -> None:
        """Attach map images to a JIRA ticket."""
        map_plotter = MapPlotter(self.engine)
        img_buffer = map_plotter.buffer_io_plot_map(trial.name)
        if img_buffer is not None:
            self.trias_jira.jira_connection.add_attachment(ticket_key, img_buffer, filename="management_zones_map.png")
        else:
            logger.critical(f"No field data available for trial {trial.name}. No map attached to ticket {ticket_key}.")

    def search_user(self, user_email: str) -> Optional[Tuple[str, str]]:
        """Search for a user by email and return their key and name.

        Args:
            user_email (str): The email of the user to search for.

        Returns:
            Optional[Tuple[str, str]]: A tuple containing the user's key and name, or None if no user is found.
        """
        return search_user(self.trias_jira, user_email)

    def handle_subtask_done(self, subtask: JiraSubTask) -> None:
        """Handle the event when a subtask is moved to 'Done' status."""
        try:
            if subtask.status_name == "Done":
                logger.info(f"Subtask {subtask.subtask_key} is marked as 'Done'. Triggering the gobbler flow.")
                # trigger_gobbler_flow(subtask)
        except Exception as e:
            logger.error(f"Error handling subtask {subtask.subtask_key} marked as 'Done': {e}")


@dataclass
class JiraTransitionManager:
    """Class for managing Jira issue transitions."""

    trias_jira: TriasJira

    def update_status(self, issue_key: str, transition_id: str) -> None:
        """Update the status of a Jira issue."""
        self.trias_jira.jira_connection.transition_issue(issue_key, transition_id)

    def transition_issue(self, issue_key: str, target_status: str) -> None:
        """Transition an issue to the target status."""
        issue = self.trias_jira.jira_connection.issue(issue_key)
        current_status_id = issue.fields.status.id

        if issue.fields.issuetype.name == "Sub-task":
            self.transition_subtask(issue_key, current_status_id, target_status)
        else:
            self.transition_standard_issue(issue_key, current_status_id, target_status)

    def transition_standard_issue(self, issue_key: str, current_status_id: str, target_status: str) -> None:
        """Transition a standard issue to the target status."""
        target_status_id = STATUS_MAPPING[target_status]  # Get the target status ID from the mapping

        while current_status_id != target_status_id:
            if current_status_id in WORKFLOW_TRANSITIONS:
                possible_transitions = WORKFLOW_TRANSITIONS[current_status_id]
                if target_status in possible_transitions:
                    next_transition_id = possible_transitions[target_status]
                else:
                    # If the direct transition is not available, take the first available transition
                    next_transition_id = list(possible_transitions.values())[0]
                logger.info(
                    f"Transitioning issue {issue_key} from {current_status_id} to next status with transition ID {next_transition_id}"
                )
                self.update_status(issue_key, next_transition_id)
                issue = self.trias_jira.jira_connection.issue(issue_key)
                current_status_id = issue.fields.status.id
            else:
                raise ValueError(f"No transition defined for status ID {current_status_id}")

    def transition_subtask(self, issue_key: str, current_status_id: str, target_status: str) -> None:
        """Transition a subtask to the target status."""
        target_status_id = SUBTASKS_STATUS_MAPPING[target_status]  # Get the target status ID from the subtask mapping

        while current_status_id != target_status_id:
            if current_status_id not in SUBTASKS_WORKFLOW:
                raise ValueError(f"No transition defined for status ID {current_status_id}")
            possible_transitions = SUBTASKS_WORKFLOW[current_status_id]
            next_transition_id = (
                possible_transitions[target_status_id]
                if target_status_id in possible_transitions
                else list(possible_transitions.values())[0]
            )
            logger.info(possible_transitions)
            logger.info(
                f"Transitioning sub-task { issue_key} from {current_status_id} to next status with transition ID {next_transition_id}"
            )
            self.update_status(issue_key, next_transition_id)
            issue = self.trias_jira.jira_connection.issue(issue_key)
            current_status_id = issue.fields.status.id
