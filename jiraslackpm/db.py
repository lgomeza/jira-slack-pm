import sqlite3
import datetime as dt2
import dateutil.parser
from datetime import datetime
import pytz

from google.api_core.exceptions import Conflict, NotFound
import google.cloud.bigquery as bigquery

from jira import get_all_users, get_info_from_issue, get_all_issues_by_user
from utils import get_users_info
from slack_connect import SlackClient
from config import Config


class TyBot(object):
    """
    This bot integrates tyba JIRA information into a BigQuery database
    and use Slack web API to send reports of the performance of the 
    Tyba's engineering team 
    """

    def __init__(self, project_id, db_name):
        """Initialize db class variables"""
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = "{}.{}".format(self.client.project, db_name)
        self.slack_client = SlackClient()

        try:
            dataset = bigquery.Dataset(self.dataset_id)
            dataset = self.client.create_dataset(
                dataset, timeout=30
            )  # Make an API request.
            self.dataset = dataset
        except Conflict:
            self.dataset = self.client.get_dataset(self.dataset_id)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.client = None
        self.dataset = None

    def __del__(self):
        self.client = None
        self.dataset = None

    def close(self):
        self.client = None
        self.dataset = None

    def create_table(self, table_name: str, schema: list):
        """
        Creates a table into the BigQuery project dataset
        initialized in the object, using the schema given as parameter. 
        """
        table_id = "{}.{}".format(self.dataset_id, table_name)
        try:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
        except Conflict:
            table = self.client.get_table(table_id)
        return table

    def delete_table(self, table_name: str):
        """Deletes the table given as parameter"""

        table_id = "{}.{}".format(self.dataset_id, table_name)
        try:
            self.client.delete_table(table_id)
        except NotFound:
            print("Table already deleted...")

    def insert_records(self, table_name, records: list):
        """inserts the records list into the table given as parameter"""

        table_id = "{}.{}".format(self.dataset_id, table_name)
        errors = self.client.insert_rows_json(table_id, records)
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def initialize_tables(
        self, users_table_name: str = "User", issues_table_name: str = "Issue"
    ):
        # ids to check the tables existence
        issues_table_id = "{}.{}".format(self.dataset_id, issues_table_name)
        users_table_id = "{}.{}".format(self.dataset_id, users_table_name)

        # if the user table does exist, it doesn't have to be initialized again.
        try:
            self.client.get_table(users_table_id)
            users = "Users are already uploaded"

        # if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            self.delete_table(users_table_name)
            users_schema = [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "account_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("active", "BOOL", mode="REQUIRED"),
                bigquery.SchemaField(
                    "display_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "index_date", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            ]
            print("Initializing users table...")
            users = self.create_table(users_table_name, users_schema)

        # if the user table does exist, it doesn't have to be initialized again.
        try:
            self.client.get_table(issues_table_id)
            issues = "Ready to upload new or updated issues for yesterday"

        # if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            issues_schema = [
                bigquery.SchemaField(
                    "story_points", "NUMERIC", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("stage", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("priority", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "project_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "issue_summary", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("creator", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("reporter", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("assignee", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("tester", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("issue_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "updated_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "index_date", "TIMESTAMP", mode="REQUIRED")
            ]
            print("Initializing issues table...")
            issues = self.create_table(issues_table_name, issues_schema)
        return users, issues

    # -------------------------
    # Slack tybot reports
    # -------------------------
    def send_performance_devs(self):
        """Send a congratulations message to the top 5 most produtive developers of the 
           engineer team in the last week."""

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
        query_job = self.client.query(query)
        for i, row in enumerate(query_job):
            # Row values can be accessed by field name or index.
            email, avg_points, week_bugs = row[2], row[3], row[4]
            try:
                user = self.slack_client.get_user_by_email(
                    "luisgomez@tyba.com.co")
                print(f"Usuario actual: {email}")
                congrats_messg = ""
                if i < 5:
                    # Send message to user
                    congrats_messg += f"""Felicidades :smile:, has sido uno de los top 5 del
                    equipo de ingeniería de Tyba esta semana :D, tus resultados son los siguientes:
                    - Promedio story points/día: {round(avg_points, 2)} 
                    - Bugs tuyos en la semana: {week_bugs}\n"""
                else:
                    congrats_messg += f"""Hola :slightly_smiling_face:, a continuación puedes encontrar los resultados de tu peformance en la última semana:\n
                    - Promedio story points/día: {round(avg_points, 2)} 
                    - Bugs tuyos en la semana: {week_bugs}\n"""
                    if avg_points < 0.1:
                        congrats_messg += """Un promedio de puntos menor a 0.1 puede indicar que no terminaste issues esta semana (¡no hay presión! :D) o que algunos de los issues que terminaste no tenían puntos asignados. ¡Recuerda asignar puntos a tus issues!"""
                self.slack_client.post_message_to_channel(
                    channel=user['id'], message=congrats_messg)
            except Exception as exc:
                print("SlackApiError:", exc,
                      "\nFallo en encontrar el correo: ", email)

    def send_bad_issues_report(self):
        """Report all the issues without story points to their respective owners"""

        query = f"""
                    SELECT
                    user.email,
                    issue.story_points,
                    issue.issue_name,
                    issue.issue_summary,
                    issue.priority,
                    issue.created_at,
                    issue.updated_at,
                    issue.index_date
                    FROM
                    `{self.dataset_id}.Issue` AS issue,
                    `{self.dataset_id}.User` AS user
                    WHERE
                    user.account_id = issue.assignee
                    AND issue.story_points IS NULL
                    AND issue.issue_type != "Error"
                    AND issue.stage != "Backlog"
                    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), issue.updated_at, HOUR) <= 24
                    AND issue.updated_at IN (
                        SELECT
                            MAX(updated_at)
                        FROM
                            `{self.dataset_id}.Issue` AS issue
                        GROUP BY
                            issue.issue_name)
                 """
        query_job = self.client.query(query)
        bad_issues_by_user = {}
        for row in query_job:
            user_email = row[0]

            if bad_issues_by_user.get(user_email) == None:
                bad_issues_by_user[user_email] = []
            bad_issue_name = row[2]
            bad_issue_summary = row[3]
            bad_issue_priority = row[4]
            new_bad_issue = {
                "name": bad_issue_name,
                "summary": bad_issue_summary,
                "priority": bad_issue_priority
            }
            bad_issues_by_user[user_email].append(new_bad_issue)

        for user_email in bad_issues_by_user:
            mssg = f"""¡Hola! Soy yo de nuevo :smile: \n
            Encontré algunos Issues asignados a tí sin story points :cry:, ¡por favor révisalos y agrégales los puntos para tenerlos en cuenta en el cálculo de performance!:\n"""
            for bad_issue in bad_issues_by_user[user_email]:
                mssg += " - ID del Issue: " + bad_issue["name"] + "\n"
                mssg += " - Descripción: " + bad_issue["summary"] + "\n"
                mssg += " - Prioridad: " + bad_issue["priority"] + "\n \n"
            user = self.slack_client.get_user_by_email(user_email)
            self.slack_client.post_message_to_channel(
                channel=user['id'], message=mssg)

    def send_weekly_squads_performance(self):
        query = f"""
                    SELECT
                    *
                    FROM
                    `{self.dataset_id}.{Config.WEEK_SQUAD_PERFORMANCE_TABLE}` AS week_squad_pf
                    WHERE
                    week_squad_pf.index_date = CURRENT_DATE("UTC-5:00")
                    ORDER BY
                    week_squad_pf.avg_points DESC
                 """
        squad_params = {
            "Tyba professional": Config.SLACK_SQUAD_TYBA_PROFESSIONAL,
            "Banner - Tyba Digital Colombia": Config.SLACK_SQUAD_BANNER,
            "Fury": Config.SLACK_SQUAD_FURY,
            "Parker": Config.SLACK_SQUAD_PARKER,
            "Robo": Config.SLACK_SQUAD_ROBO,
            "Groot": Config.SLACK_SQUAD_GROOT,
            "ROGERS": Config.SLACK_SQUAD_ROGERS,
            "Stark": Config.SLACK_SQUAD_STARK,
        }
        query_job = self.client.query(query)
        for row in query_job:
            squad, avg_point, week_bugs = row[0], row[1], row[2]
            print(squad)
            mssg = f"""Hola :smile:, el rendimiento de {squad} en su última semana fue:\n
            - Promedio de story points/día del equipo: {avg_point}\n
            - Total de bugs en la semana: {week_bugs}\n"""

            self.slack_client.post_message_to_channel(
                channel=squad_params[squad], message=mssg)
            if(week_bugs > 0):
                bugs_detail = get_weekly_squads_bug_detail(squad)
                mssg = f"""Este es un resumen de los bugs en producción de la semana:\n"""
                for row in bugs_detail:
                    summary, issue_id, project_name, assignee = row[0], row[1], row[2], row[3]
                    mssg += f"""{issue_id} - {summary}: \n
                    - Persona asignada: {assignee} \n \n"""
                self.slack_client.post_message_to_channel(
                    channel=squad_params[squad], message=mssg)

    def send_weekly_tyba_performance(self):
        query = f"""
                SELECT
                *
                FROM
                `{self.dataset_id}.{Config.WEEK_TYBA_PERFORMANCE_TABLE}` AS week_tyba_pf
                WHERE
                week_tyba_pf.index_date = CURRENT_DATE("UTC-5:00")
                ORDER BY
                week_tyba_pf.avg_points DESC
                """
        query_job = self.client.query(query)
        for row in query_job:
            print(row)
            avg_point, week_bugs = row[0], row[1]
            mssg = f"""¡Hola! :smile: Este es el reporte semanal de Tyba.\n
            Esta semana el equipo completo tuvo una productividad* de: {round(avg_point, 2)}.\n
            Además, se subieron un total de {week_bugs} bugs en producción. ¡Feliz Semana!
            _*La productividad se calcula como story points/tiempo de resolución en días de los issues terminados en el transcurso de la semana._
            _*Se considera terminado un issue cuando llega a dev_
            """

            self.slack_client.post_message_to_channel(
                channel=Config.SLACK_SQUAD_TYBA_EOS, message=mssg)

    def get_weekly_squads_bug_detail(self, squad_name):
        query = f"""
                         SELECT
                           processed.issue_summary,
                           processed.issue_name,
                           issue.project_name,
                           issue.assignee
                         FROM (
                           SELECT
                             issue_summary,
                             issue_name,
                             MAX(updated_at) AS updated_at
                           FROM
                             `k-ren-295903.jira.Issue`
                           WHERE
                             DATE(created_at)>=CURRENT_DATE("UTC-5:00")-14
                             AND issue_type = "Error"
                             AND project_name != "Support"
                           GROUP BY
                             issue_summary,
                             issue_name) AS processed,
                          `k-ren-295903.jira.Issue` AS issue
                         WHERE
                           issue.issue_name = processed.issue_name
                           AND issue.updated_at = processed.updated_at
                           AND issue.project_name = {squad_name}
                    """
        return self.client.query(query)

# -------------
# End of tybot
# -------------

def load_users_into_bigquery(project_id, database_name):
    with TyBot(project_id, database_name) as db:
        db.delete_table("User")
        users_table_id = "{}.{}".format(db.dataset_id, "User")
        # if the user table does exist, it doesn't have to be initialized again.
        try:
            db.client.get_table(users_table_id)
            users = "Users are already uploaded"

        # if it doesn't exist, it has to be created with the following schema.
        except NotFound:
            users_schema = [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "account_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("active", "BOOL", mode="REQUIRED"),
                bigquery.SchemaField(
                    "display_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "index_date", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            ]
            print("Initializing users table...")
            users = db.create_table("User", users_schema)
            users = get_all_users()
            info_users = get_users_info()
            u = datetime.utcnow()
            now = u.replace(tzinfo=pytz.timezone("America/Bogota"))

        for user in users:
            if user["accountType"] == "atlassian" and user['active'] == True:
                db.insert_records(
                    "User",
                    [
                        {
                            "account_id": user["accountId"],
                            "account_type": user["accountType"],
                            "active": user["active"],
                            "display_name": user["displayName"],
                            "index_date": str(now),
                            "email": info_users[info_users['id'] == user["accountId"]]['email'].iloc[0],
                        }
                    ],
                )
                print("Inserted user with ID {} and name {}".format(
                    user["accountId"], user["displayName"]))


def load_new_issues_into_bigquery(project_id, database_name):
    with TyBot(project_id, database_name) as db:
        users_table, issues_table = db.initialize_tables()
        print(users_table, issues_table)
        users = get_all_users()
        u = datetime.utcnow()
        now = u.replace(tzinfo=pytz.timezone("America/Bogota"))
        for user in users:

            if user["accountType"] == "atlassian":

                issues = get_all_issues_by_user(user["accountId"])
                records = []
                for issue in issues:
                    parsed_issue = get_info_from_issue(issue)
                    parsed_issue["assignee"] = user["accountId"]
                    parsed_issue["index_date"] = str(now)
                    created_date = dateutil.parser.parse(
                        parsed_issue["created_at"])
                    updated_date = dateutil.parser.parse(
                        parsed_issue["updated_at"])

                    last_day_date = now - dt2.timedelta(1)
                    if created_date > last_day_date or updated_date > last_day_date:
                        records.append(parsed_issue)

                if records:
                    db.insert_records("Issue", records)
                print(
                    "Inserted {} new issues for user ID: {}".format(
                        len(records), user["accountId"]
                    )
                )
            else:
                print(
                    "User with ID {} is of type {}. Skipping issues fetch..".format(
                        user["accountId"], user["accountType"]
                    )
                )
