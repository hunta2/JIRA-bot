"""
This module contains helper functions for JIRA bot operations.
"""

import re
from io import BytesIO
from dataclasses import dataclass
from typing import NamedTuple, Optional, Union, List, Tuple
import pandas as pd
import sqlalchemy as sa
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import contextily as cx
from loguru import logger
from jira_bot.lib.query.database import (
    get_record_by_name,
    get_feature,
    get_record_by_id,
    upsert_epic,
    upsert_trial,
    upsert_subtask,
)
from jira_bot.lib.core.jira_connections import (
    JiraEpic,
    JiraIssue,
    TriasJira,
)
from jira_bot.lib.tools.constants import MZ_COLUMNS

class Trial(NamedTuple):
    """NamedTuple for a itertuple of a trial"""

    name: str
    value: int


def create_jira_description(
    jira_description: Optional[str],
    db_description_result: pd.DataFrame,
    columns: list,
    epic: bool = True,
    farm_name: Optional[str] = None,
    field_name: Optional[str] = None,
) -> str:
    """Create a Jira ticket description string from the protocol or trial data and update the description."""
    if jira_description is None:
        jira_description = ""

    if field_name:
        jira_description = f"Field Name: {field_name}\n" + jira_description

    if farm_name:
        jira_description = f"Farm Name: {farm_name}\n" + jira_description

    if not isinstance(db_description_result, pd.DataFrame):
        db_description_result = pd.DataFrame([db_description_result._asdict()])

    if db_description_result.empty:
        return jira_description

    # "blank" comments are breaking coloumn/rows
    db_description_result.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
    # Need white space to make markdown 'fill' the empty cell
    db_description_result = db_description_result.fillna(" ")

    table_row = db_description_result.iloc[0]
    rows = []
    for i in range(0, len(columns), 3):
        row = columns[i : i + 3]
        row_values = [str(table_row[col]) if col in table_row else " " for col in row]
        rows.append(f"||{'||'.join(row)}||\n|{'|'.join(row_values)}|\n")

    description_template = "".join(rows)
    if epic:
        table_pattern = re.compile(
            r"\|\|uuid\|\|date_created\|\|last_updated\|\|.*?\|\|flow_run_id\|\|",
            re.DOTALL,
        )
    else:
        table_pattern = re.compile(
            r"\|\|uuid\|\|date_created\|\|last_updated\|\|.*?\|\|is_abandoned\|\|",
            re.DOTALL,
        )
    # Keep any test entry strings, but drop everything after the table
    if match := table_pattern.search(jira_description):
        jira_description = jira_description[: match.start()]
    # just insert a new table
    jira_description += "\n" + description_template

    return jira_description


def normalize_and_sort_data(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Normalize and sort the data in the DataFrame according to the schema."""
    for column, dtype in schema.items():
        if column in df.columns:
            if "VARCHAR" in dtype or "TEXT" in dtype:
                df[column] = df[column].astype(str)
            elif "INTEGER" in dtype:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif "NUMERIC" in dtype:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif "TIMESTAMP" in dtype or "DATE" in dtype:
                df[column] = pd.to_datetime(df[column], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    if "components" in df.columns:
        df["components"] = df["components"].apply(lambda x: str(sorted(x)) if isinstance(x, (list, set)) else x)
    if "labels" in df.columns:
        df["labels"] = df["labels"].apply(lambda x: str(sorted(x)) if isinstance(x, (list, set)) else x)
    df = df[schema.keys()]
    return df


@dataclass
class MapPlotter:
    """Class to plot a field boundary and plot zones with contextily."""

    engine: sa.engine
    zoom_out_factor: float = 3.0

    def buffer_io_plot_map(self, trial_name: str):
        """Plot the geodata and field on a map with contextily."""
        try:
            trial = get_record_by_name(self.engine, "trial", "name", trial_name)
            if not trial.empty:
                field_uuid = trial.iloc[0]["field_uuid"]
            else:
                logger.warning("No trial found for name %s", trial_name)
                return None
            management_zones = get_feature(self.engine, "management_zones", field_uuid, query_column="fieldUuid", to_utm=False)
            if not management_zones.empty:
                management_zones = management_zones[MZ_COLUMNS]
            else:
                logger.warning(f"No management zones found for field {field_uuid}")
                return None
            field = get_feature(self.engine, "fields", field_uuid, to_utm=False)

            if management_zones.crs != "EPSG:4326":
                management_zones = management_zones.to_crs("EPSG:4326")
            if field.crs != "EPSG:4326":
                field = field.to_crs("EPSG:4326")

            bounds = field.total_bounds
            center_lon = (bounds[0] + bounds[2]) / 2
            center_lat = (bounds[1] + bounds[3]) / 2
            width = (bounds[2] - bounds[0]) * self.zoom_out_factor
            height = (bounds[3] - bounds[1]) * self.zoom_out_factor
            new_bounds = [
                center_lon - width / 2,
                center_lat - height / 2,
                center_lon + width / 2,
                center_lat + height / 2,
            ]

            fig, ax = plt.subplots(figsize=(10, 10))

            management_zones.plot(ax=ax, alpha=0.3, edgecolor="k", linewidth=3.5)
            field.boundary.plot(ax=ax, edgecolor="red", linewidth=2)
            ax.set_xlim(new_bounds[0], new_bounds[2])
            ax.set_ylim(new_bounds[1], new_bounds[3])
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_frame_on(False)
            plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

            cx.add_basemap(ax, source=cx.providers.Esri.WorldImagery, crs="EPSG:4326")
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format="png", bbox_inches="tight", pad_inches=0)
            img_buffer.seek(0)

            return img_buffer

        except Exception as e:
            logger.warning(f"Failed to plot map for trial {trial_name}: {e}")
            return None


def compare_fields(
    jira_ticket_db: pd.DataFrame,
    jira_dict: list[str],
    jira_ticket: Union[JiraEpic, JiraIssue],
    jira_schema: dict[str, str],
) -> bool:
    """Compare fields between JIRA ticket and database record."""
    jira_ticket_db.drop(columns=["version", "update_timestamp"], inplace=True)
    ticket_dict = {field: getattr(jira_ticket, field) for field in jira_dict}
    ticket_df = pd.DataFrame([ticket_dict])
    return normalize_sort_and_compare(ticket_df, jira_ticket_db, jira_schema)


def normalize_sort_and_compare(jira_df: pd.DataFrame, db_df: pd.DataFrame, schema: dict) -> bool:
    """Normalize, sort and compare the data."""
    db_df_normalized_sorted = normalize_and_sort_data(db_df, schema)
    jira_df_normalized_sorted = normalize_and_sort_data(jira_df, schema)
    return not db_df_normalized_sorted.iloc[-1].equals(jira_df_normalized_sorted.iloc[-1])


def create_labels(epic: JiraEpic) -> List[str]:
    """Create labels based on the epic information and retain original labels."""
    parts = epic.protocol_id.split("-")
    new_labels = parts[:-1]
    updated_labels = list(epic.labels)
    for label in new_labels:
        if label not in updated_labels:
            updated_labels.append(label)
    return updated_labels


def search_user(trias_jira: TriasJira, user_email: str) -> Optional[Tuple[str, str]]:
    """Search for a user by email and return their key and name."""
    if user_email:
        users = trias_jira.jira_connection.search_users(user=user_email)
        if users:
            user = users[0]
            return user.key, user.name
        return tuple()
    else:
        return tuple()

def create_jira_ticket(
    trias_jira: TriasJira,
    project_id: str,
    summary: str,
    description: str,
    issue_type: str,
    parent_key: Optional[str],
    labels: List[str],
    assignee: str,
    custom_fields: dict,
) -> Optional[str]:
    """Create a JIRA ticket with the given fields."""
    fields = {
        "project": {"id": project_id},
        "summary": summary,
        "description": description,
        "issuetype": {"name": issue_type},
        "labels": labels,
        "assignee": {"name": assignee},
    }
    if parent_key:
        fields["parent"] = {"key": parent_key}
    fields.update(custom_fields)

    try:
        new_issue = trias_jira.jira_connection.create_issue(fields)
        logger.info(f"Ticket created: {new_issue.key}")
        return new_issue.key
    except Exception as e:
        logger.error(f"Failed to create ticket: {e}")
        return None


def upsert_record(
    engine: sa.engine,
    table_name: str,
    record_dict: dict,
    record_id: str,
    id_field: str,
    schema: dict,
    new_version: bool = False,
):
    """Upsert a record into the database."""
    try:
        record_df = pd.DataFrame([record_dict])
        # record_df = normalize_and_sort_data(record_df, schema)  # Ensure data conforms to the schema
        existing_record = get_record_by_id(engine, table_name, id_field, record_id)

        if not existing_record.empty:
            record_df["version"] = (
                existing_record["version"].iloc[-1] + 1 if new_version else existing_record["version"].iloc[-1]
            )
        else:
            record_df["version"] = 0

        logger.info(f"Upserting record {record_id} into the {table_name} table.")
        if table_name == "epics":
            upsert_epic(engine, record_df)
        elif table_name == "issues":
            upsert_trial(engine, record_df)
        elif table_name == "sub_tasks":
            upsert_subtask(engine, record_df)
    except Exception as e:
        logger.error(f"Error upserting record {record_id} into the {table_name} table: {e}")
