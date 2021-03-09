from config import Config

DATASET_ID = "{}.{}".format(Config.BQ_PROJECT,Config.BQ_DATABASE)

def performance_devs():
    query = f"""
    SELECT
    *
    FROM
    `{self.dataset_id}.{Config.WEEK_DEVS_PERFORMANCE_TABLE}` AS week_dev_pf
    WHERE
    week_dev_pf.index_date = CURRENT_DATE("UTC-5:00")
    ORDER BY
    week_dev_pf.avg_points DESC
    """

    return query

def func():
    query = f"""
    """

    return query

def func():
    query = f"""
    """

    return query

def warning_issues_dev():
    query = f"""
        SELECT
            issue.issue_name,
            issue.issue_summary,
            user.email,
            processed.days_in_DEV,
            sprint.start_date,
            sprint.end_date,
            FROM (
            SELECT
                issue.issue_name,
                sprint.name,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(updated_at), DAY) AS days_in_DEV,
                MAX(updated_at) AS last_update
            FROM
                `{DATASET_ID}.Issue` AS issue,
                `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
            WHERE
                issue.sprint_name = sprint.name
                AND sprint.start_date <= CURRENT_TIMESTAMP()
                AND sprint.end_date >= CURRENT_TIMESTAMP()
                AND issue.stage = "ENV: DEV"
                AND issue.issue_type != "Error"
                AND issue.issue_type != "Bug"
                AND issue.issue_name IN (
                SELECT
                issue.issue_name
                FROM (
                SELECT
                    issue_id,
                    issue_name,
                    MAX(updated_at) AS last_update
                FROM
                    `{DATASET_ID}.Issue`
                GROUP BY
                    issue_id,
                    issue_name) AS issues_last_updated,
                `{DATASET_ID}.Issue` AS issue
                WHERE
                issue.issue_id = issues_last_updated.issue_id
                AND issue.updated_at = issues_last_updated.last_update
                AND issue.sprint_status = "active"
                AND issue.sprint_name IS NOT NULL
                AND issue.stage = "ENV: DEV")
            GROUP BY
                issue.issue_name,
                sprint.name) AS processed,
            `{DATASET_ID}.Issue` AS issue,
            `{DATASET_ID}.User` AS user,
            `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
            WHERE
            issue.issue_name = processed.issue_name
            AND processed.last_update = issue.updated_at
            AND issue.assignee = user.account_id
            AND issue.sprint_name = sprint.name
            AND processed.days_in_DEV > 3
        """

    return query

def warning_issues_qa():
    query = f"""
    SELECT
        issue.issue_name,
        issue.issue_summary,
        user.email,
        processed.days_in_QA,
        sprint.start_date,
        sprint.end_date,
        FROM (
        SELECT
            issue.issue_name,
            sprint.name,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(updated_at), DAY) AS days_in_QA,
            MAX(updated_at) AS last_update
        FROM
            `{DATASET_ID}.Issue` AS issue,
            `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
        WHERE
            issue.sprint_name = sprint.name
            AND sprint.start_date <= CURRENT_TIMESTAMP()
            AND sprint.end_date >= CURRENT_TIMESTAMP()
            AND issue.stage = "ENV: QA"
            AND issue.issue_type != "Error"
            AND issue.issue_type != "Bug"
            AND issue.issue_name IN (
            SELECT
            issue.issue_name
            FROM (
            SELECT
                issue_id,
                issue_name,
                MAX(updated_at) AS last_update
            FROM
                `{DATASET_ID}.Issue`
            GROUP BY
                issue_id,
                issue_name) AS issues_last_updated,
            `{DATASET_ID}.Issue` AS issue
            WHERE
            issue.issue_id = issues_last_updated.issue_id
            AND issue.updated_at = issues_last_updated.last_update
            AND issue.sprint_status = "active"
            AND issue.sprint_name IS NOT NULL
            AND issue.stage = "ENV: QA")
        GROUP BY
            issue.issue_name,
            sprint.name) AS processed,
        `{DATASET_ID}.Issue` AS issue,
        `{DATASET_ID}.User` AS user,
        `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
        WHERE
        issue.issue_name = processed.issue_name
        AND processed.last_update = issue.updated_at
        AND (CASE WHEN issue.tester IS NULL THEN issue.assignee ELSE issue.tester END) = user.account_id
        AND issue.sprint_name = sprint.name
        AND processed.days_in_QA > 3
    """
    
    return query

def warning_issues_ready_dev():
    query=f"""
    SELECT
        issue.issue_name,
        issue.issue_summary,
        user.email,
        processed.days_in_ready_DEV,
        sprint.start_date,
        sprint.end_date,
        FROM (
        SELECT
            issue.issue_name,
            sprint.name,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MIN(updated_at), DAY) AS days_in_ready_DEV,
            MAX(updated_at) AS last_update
        FROM
            `{DATASET_ID}.Issue` AS issue,
            `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
        WHERE
            issue.sprint_name = sprint.name
            AND sprint.start_date <= CURRENT_TIMESTAMP()
            AND sprint.end_date >= CURRENT_TIMESTAMP()
            AND LOWER(issue.stage) = "ready for dev"
            AND issue.issue_type != "Error"
            AND issue.issue_type != "Bug"
            AND issue.issue_name IN (
            SELECT
            issue.issue_name
            FROM (
            SELECT
                issue_id,
                issue_name,
                MAX(updated_at) AS last_update
            FROM
                `{DATASET_ID}.Issue`
            GROUP BY
                issue_id,
                issue_name) AS issues_last_updated,
            `{DATASET_ID}.Issue` AS issue
            WHERE
            issue.issue_id = issues_last_updated.issue_id
            AND issue.updated_at = issues_last_updated.last_update
            AND issue.sprint_status = "active"
            AND issue.sprint_name IS NOT NULL
            AND LOWER(issue.stage) = "ready for dev")
        GROUP BY
            issue.issue_name,
            sprint.name) AS processed,
        `{DATASET_ID}.Issue` AS issue,
        `{DATASET_ID}.User` AS user,
        `{DATASET_ID}.{Config.SPRINT_TABLE}` AS sprint
        WHERE
        issue.issue_name = processed.issue_name
        AND processed.last_update = issue.updated_at
        AND issue.assignee = user.account_id
        AND issue.sprint_name = sprint.name
        AND processed.days_in_ready_DEV >= 7
  """

    return query