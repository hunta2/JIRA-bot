import pandas as pd
import sqlalchemy as sa
import typing as t
from pydantic import BaseModel, Field
import boto3
from awswrangler.secretsmanager import get_secret_json
from loguru import logger
from datetime import datetime as dt
from datetime import timezone
import geopandas as gpd


class DBCredentials(BaseModel):
    username: str
    password: str
    host: t.Optional[str] = None
    port: t.Optional[int] = None
    database: t.Optional[str] = Field(default=None, alias="dbname")

    class Config:
        populate_by_name = True

    @classmethod
    def from_sa_url(cls, url: sa.engine.URL) -> "DBCredentials":
        return cls(
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=url.database,
        )


def get_engine(database: str, read_session: boto3.Session, db_parameters: dict = None) -> sa.Engine:
    """Create a Postgres engine from a secret stores on AWS Secrets Manager

    Args:
        database (str): name of secret with DB credentials
        read_session (boto3.Session): session to use for reading the secret

    Returns:
        sa.Engine: SQLAlchemy engine
    """
    logger.debug("Retrieving Postgres Secret %s" % database)
    credentials = DBCredentials(**get_secret_json(database, read_session))
    if db_parameters:
        credentials = credentials.model_copy(update=db_parameters)
    logger.debug("Secret retrieved")
    logger.debug(credentials)
    engine = sa.create_engine(
        sa.URL.create(
            drivername="postgresql+psycopg2",
            username=credentials.username,
            password=credentials.password,
            host=credentials.host,
            port=credentials.port,
            database=credentials.database,
        ),
        echo=False,
    )
    return engine


def get_record_by_name(engine: sa.engine, table_name: str, column_name: str, value: str) -> pd.DataFrame:
    """Retrieve record information from the database by name.

    Args:
        engine (sa.engine): SQLAlchemy engine
        table_name (str): Name of the table to query
        column_name (str): Name of the column to filter by
        value (str): Value to filter by

    Returns:
        pd.DataFrame: Record information from the database
    """
    query = f"SELECT * FROM {table_name} WHERE {column_name} = :value LIMIT 1"
    with engine.connect() as con:
        df = pd.read_sql_query(sql=sa.text(query), params={"value": value}, con=con)
    if df.empty:
        logger.info(f"Record with {column_name} {value} not found in {table_name}.")
    return df


def get_record_by_id(engine: sa.engine, table_name: str, column_name: str, value: str) -> pd.DataFrame:
    """Retrieve record information from the database by ID.

    Args:
        engine (sa.engine): SQLAlchemy engine
        table_name (str): Name of the table to query
        column_name (str): Name of the column to filter by
        value (str): Value to filter by

    Returns:
        pd.DataFrame: Record information from the database
    """
    query = f"SELECT * FROM {table_name} WHERE {column_name} = :value LIMIT 1"
    with engine.connect() as con:
        df = pd.read_sql_query(sql=sa.text(query), params={"value": value}, con=con)
    if df.empty:
        logger.info(f"Record with {column_name} {value} not found in {table_name}.")
    return df


def get_record_by_uuid(engine: sa.engine, table_name: str, column_name: str, value: str) -> pd.DataFrame:
    """Retrieve record information from the database by UUID.

    Args:
        engine (sa.engine): SQLAlchemy engine
        table_name (str): Name of the table to query
        column_name (str): Name of the column to filter by
        value (str): Value to filter by

    Returns:
        pd.DataFrame: Record information from the database
    """
    query = f'SELECT * FROM {table_name} WHERE "{column_name}" = :value'
    with engine.connect() as con:
        df = pd.read_sql_query(sql=sa.text(query), params={"value": value}, con=con)
    if df.empty:
        logger.info(f"No records found for {column_name} {value} in {table_name}.")
    return df


def query_farm_field_names(engine: sa.engine, uuid: str, entity: str) -> str:
    """Query the name of a farm or field based on the UUID.

    Args:
        engine (sa.engine): The database engine.
        uuid (str): The UUID of the farm or field.
        entity (str): The entity type, either 'farm' or 'field'.

    Returns:
        str: The name of the farm or field, or an empty string if not found.
    """
    if entity == "farm":
        table_name = "public.farm"
        select_columns = "name"
    else:
        table_name = "public.fields"
        select_columns = 'name, "farmUuid"'  # Ensure the correct column name is used

    with engine.connect() as con:
        sql_query = sa.text(
            f"""
            SELECT {select_columns} FROM {table_name}
            WHERE "uuid" = :uuid
            """
        )
        result = pd.read_sql_query(sql=sql_query, con=con, params={"uuid": uuid})

    if not result.empty:
        if entity == "farm":
            return result["name"].iloc[0]
        else:
            return result["name"].iloc[0], result["farmUuid"].iloc[0]

    logger.info(f"No name found for {entity} with UUID {uuid}.")
    return "" if entity == "farm" else ("", "")


def get_feature(
    engine: sa.engine.Engine,
    table: str,
    uuid: str,
    query_column: str = "uuid",
    to_utm: bool = True,
) -> gpd.GeoDataFrame:
    query = sa.text(f'SELECT * FROM {table} WHERE "{query_column}" = :uuid')
    
    # Execute the query with the uuid as a parameter
    with engine.connect() as connection:
        feature = gpd.read_postgis(query, connection, geom_col="geom", params={"uuid": uuid})
    
    if feature.empty:
        return gpd.GeoDataFrame()
    
    if to_utm:
        utm_epsg = feature.estimate_utm_crs().to_epsg()
        feature = feature.to_crs(utm_epsg)
    
    return feature


def upsert_epic(engine: sa.engine, epic_df: pd.DataFrame) -> None:
    """Upsert the epic information into the database.
    Args:
        engine (sa.engine): The database engine.
        epic_df (pd.DataFrame): The DataFrame containing epic information.
    Returns:
        None"""
    insert_statement = sa.text(
        """
        INSERT INTO epics ("protocol_uuid", "last_updated", "epic_id", "epic_key", "summary", "version", 
            "protocol_id", "requestor_email", "assignee_email", "trial_engineer_email", "protocol_sheet", 
            "year_of_harvest", "country", "crop", "business_case", "trial_type", "trial_objective", "budget", "paid_costs", "forcasted_costs","planned_trials", "executed_trials", 
            "sponsor", "cost_sheet", "url_field", "components", "created", 
            "updated", "last_viewed", "watch_count", "labels", "status_name", "status_id",          "status_category", "creator_email", "comments_count", "due_date", "update_timestamp"
        ) VALUES (
            :protocol_uuid, :last_updated, :epic_id, :epic_key, :summary, :version, 
            :protocol_id, :requestor_email, :assignee_email, :trial_engineer_email, :protocol_sheet, 
            :year_of_harvest, :country, :crop, :business_case, :trial_type, :trial_objective, :budget, :paid_costs, :forcasted_costs, :planned_trials, :executed_trials,
            :sponsor, :cost_sheet, :url_field, :components, :created, 
            :updated, :last_viewed, :watch_count, :labels, :status_name, :status_id,
            :status_category, :creator_email, :comments_count, :due_date, :update_timestamp
        ) ON CONFLICT ("epic_key") DO UPDATE SET
            "protocol_uuid" = EXCLUDED."protocol_uuid",
            "last_updated" = EXCLUDED."last_updated",
            "epic_id" = EXCLUDED."epic_id",
            "summary" = EXCLUDED."summary",
            "version" = EXCLUDED."version",
            "protocol_id" = EXCLUDED."protocol_id",
            "assignee_email" = EXCLUDED."assignee_email",
            "trial_engineer_email" = EXCLUDED."trial_engineer_email",
            "requestor_email" = EXCLUDED."requestor_email",
            "protocol_sheet" = EXCLUDED."protocol_sheet",
            "year_of_harvest" = EXCLUDED."year_of_harvest",
            "country" = EXCLUDED."country",
            "crop" = EXCLUDED."crop",
            "business_case" = EXCLUDED."business_case",
            "trial_type" = EXCLUDED."trial_type",
            "trial_objective" = EXCLUDED."trial_objective",
            "budget" = EXCLUDED."budget",
            "paid_costs" = EXCLUDED."paid_costs",
            "forcasted_costs" = EXCLUDED."forcasted_costs",
            "planned_trials" = EXCLUDED."planned_trials",
            "executed_trials" = EXCLUDED."executed_trials",
            "sponsor" = EXCLUDED."sponsor",
            "cost_sheet" = EXCLUDED."cost_sheet",
            "url_field" = EXCLUDED."url_field",
            "components" = EXCLUDED."components",
            "created" = EXCLUDED."created",
            "updated" = EXCLUDED."updated",
            "last_viewed" = EXCLUDED."last_viewed",
            "watch_count" = EXCLUDED."watch_count",
            "labels" = EXCLUDED."labels",
            "status_name" = EXCLUDED."status_name",
            "status_id" = EXCLUDED."status_id",
            "status_category" = EXCLUDED."status_category",
            "creator_email" = EXCLUDED."creator_email",
            "comments_count" = EXCLUDED."comments_count",
            "due_date" = EXCLUDED."due_date",
            "update_timestamp" = EXCLUDED."update_timestamp";
        """
    )

    with engine.connect() as con:
        for _, epic in epic_df.iterrows():
            epic_dict = epic.to_dict()
            epic_dict["update_timestamp"] = dt.now(timezone.utc)
            for date_column in ["created", "updated", "last_viewed", "last_updated", "due_date", "update_timestamp"]:
                if isinstance(epic_dict[date_column], dt):
                    epic_dict[date_column] = epic_dict[date_column].strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Upserting epic with data: {epic_dict}")
            con.execute(insert_statement, epic_dict)
            con.commit()


def upsert_trial(engine: sa.engine, trial_df: pd.DataFrame) -> None:
    insert_statement = sa.text(
        """
        INSERT INTO issues (
            "trial_uuid", "trial_id", "created", "version", "updated", "last_viewed", "watch_count", 
            "labels", "status_name", "status_id", "status_category", "summary", "creator_email", 
            "comments_count", "due_date", "issue_id", "issue_key", 
            "trial_engineer_email", "last_updated", "epic_link", "requestor_email", "assignee_email", "subtask_ids", "subtask_keys", "update_timestamp"
        ) VALUES (
            :trial_uuid, :trial_id, :created, :version, :updated, :last_viewed, :watch_count, 
            :labels, :status_name, :status_id, :status_category, :summary, :creator_email, 
            :comments_count, :due_date, :issue_id, :issue_key, 
            :trial_engineer_email, :last_updated, :epic_link, :requestor_email, :assignee_email, :subtask_ids, :subtask_keys, :update_timestamp
        ) ON CONFLICT ("uuid") DO UPDATE SET
            "trial_uuid" = EXCLUDED."trial_uuid",
            "trial_id" = EXCLUDED."trial_id",
            "created" = EXCLUDED."created",
            "version" = EXCLUDED."version",
            "updated" = EXCLUDED."updated",
            "last_viewed" = EXCLUDED."last_viewed",
            "watch_count" = EXCLUDED."watch_count",
            "labels" = EXCLUDED."labels",
            "status_name" = EXCLUDED."status_name",
            "status_id" = EXCLUDED."status_id",
            "status_category" = EXCLUDED."status_category",
            "summary" = EXCLUDED."summary",
            "creator_email" = EXCLUDED."creator_email",
            "comments_count" = EXCLUDED."comments_count",
            "due_date" = EXCLUDED."due_date",
            "issue_id" = EXCLUDED."issue_id",
            "issue_key" = EXCLUDED."issue_key",
            "trial_engineer_email" = EXCLUDED."trial_engineer_email",
            "last_updated" = EXCLUDED."last_updated",
            "epic_link" = EXCLUDED."epic_link",
            "requestor_email" = EXCLUDED."requestor_email",
            "assignee_email" = EXCLUDED."assignee_email",
            "subtask_ids" = EXCLUDED."subtask_ids",
            "subtask_keys" = EXCLUDED."subtask_keys",
            "update_timestamp" = EXCLUDED."update_timestamp"
            ;
        """
    )
    with engine.connect() as con:
        for _, trial in trial_df.iterrows():
            issue_dict = trial.to_dict()
            issue_dict["update_timestamp"] = dt.now(timezone.utc)
            for date_column in ["created", "updated", "last_viewed", "update_timestamp", "due_date", "last_updated"]:
                if isinstance(issue_dict[date_column], dt):
                    issue_dict[date_column] = issue_dict[date_column].strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Upserting issue with data: {issue_dict}")
            con.execute(insert_statement, issue_dict)
            con.commit()


def upsert_subtask(engine: sa.engine, subtask_df: pd.DataFrame) -> None:
    insert_statement = sa.text(
        """           
        INSERT INTO public.sub_tasks (
            parent_issue,
            subtask_key,
            file_uuid,
            trial_id,
            created,
            updated,
            last_viewed,
            watch_count,
            labels,
            status_name,
            status_category,
            status_id,
            summary,
            version,
            creator_email,
            comments_count,
            trial_engineer_email,
            assignee_email,
            update_timestamp
        ) VALUES (
            :parent_issue,
            :subtask_key,
            :file_uuid,
            :trial_id,
            :created,
            :updated,
            :last_viewed,
            :watch_count,
            :labels,
            :status_name,
            :status_category,
            :status_id,
            :summary,
            :version,
            :creator_email,
            :comments_count,
            :trial_engineer_email,
            :assignee_email,
            :update_timestamp
        )
        ON CONFLICT (subtask_key) DO UPDATE SET
            parent_issue = EXCLUDED.parent_issue,
            subtask_key = EXCLUDED.subtask_key,
            file_uuid = EXCLUDED.file_uuid,
            trial_id = EXCLUDED.trial_id,
            created = EXCLUDED.created,
            updated = EXCLUDED.updated,
            last_viewed = EXCLUDED.last_viewed,
            watch_count = EXCLUDED.watch_count,
            labels = EXCLUDED.labels,
            status_name = EXCLUDED.status_name,
            status_category = EXCLUDED.status_category,
            status_id = EXCLUDED.status_id,
            summary = EXCLUDED.summary,
            version = EXCLUDED.version,
            creator_email = EXCLUDED.creator_email,
            comments_count = EXCLUDED.comments_count,
            trial_engineer_email = EXCLUDED.trial_engineer_email,
            assignee_email = EXCLUDED.assignee_email,
            update_timestamp = EXCLUDED.update_timestamp;
        """
    )
    with engine.connect() as con:
        for _, subtask in subtask_df.iterrows():
            subtask_dict = subtask.to_dict()
            subtask_dict["update_timestamp"] = dt.now(timezone.utc)
            for date_column in ["created", "updated", "last_viewed", "update_timestamp"]:
                if isinstance(subtask_dict[date_column], dt):
                    subtask_dict[date_column] = subtask_dict[date_column].strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Upserting issue with data: {subtask_dict}")
            con.execute(insert_statement, subtask_dict)
            con.commit()
