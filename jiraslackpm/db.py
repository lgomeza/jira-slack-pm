import datetime as dt2
import dateutil.parser
from datetime import datetime
import pytz

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery_storage
import google.cloud.bigquery as bigquery
import google.auth

import query_manager as query_manager
from jira import get_all_users, get_info_from_issue, get_all_issues_by_user, get_all_boards, get_all_sprints_by_board
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
        credentials, your_project_id = google.auth.default()
        self.bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
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

    def send_issues_qa_no_tester_report(self, users_with_previous_bad_issues):
        """Report all the issues without in QA without tester to their respective owners"""
        query = f"""
                    SELECT user.email, issue.issue_name, issue.issue_summary
                    FROM (SELECT
                    issue_id,
                    issue_name,
                    MAX(updated_at) AS last_update
                    FROM
                    `{self.dataset_id}.Issue`
                    GROUP BY
                      issue_id, issue_name) AS issues_last_update,
                      `{self.dataset_id}.Issue` AS issue,
                      `{self.dataset_id}.User` AS user,
                      `{self.dataset_id}.{Config.SPRINT_TABLE}` AS sprint
                      WHERE issue.issue_id = issues_last_update.issue_id
                      AND issue.issue_name = issues_last_update.issue_name
                      AND issue.updated_at = issues_last_update.last_update
                      AND issue.assignee = user.account_id
                      AND issue.sprint_name = sprint.name
                      AND sprint.start_date <= CURRENT_TIMESTAMP() 
                      AND sprint.end_date >= CURRENT_TIMESTAMP() 
                      AND issue.sprint_status = "active"
                      AND issue.stage = "ENV: QA"
                      AND issue.tester IS NULL
                 """
        query_job = self.client.query(query)
        bad_issues_by_user = {}
        for row in query_job:
            user_email = row[0]

            if bad_issues_by_user.get(user_email) == None:
                bad_issues_by_user[user_email] = []
            bad_issue_name = row[1]
            bad_issue_summary = row[2]
            new_bad_issue = {
                "name": bad_issue_name,
                "summary": bad_issue_summary
            }
            bad_issues_by_user[user_email].append(new_bad_issue)

        for user_email in bad_issues_by_user:
            if(user_email in users_with_previous_bad_issues):
                mssg = f"""También encontré algunos Issues en QA a los que no les fue asignado un tester. Por favor revísalos y en lo posible agregales un tester :smile::\n"""
            else:
                mssg = f"""¡Hola! Soy yo de nuevo :smile: \n
            Encontré algunos Issues en QA a los que no les fue asignado un tester. Por favor revísalos y en lo posible agregales un tester :smile::\n"""
            for bad_issue in bad_issues_by_user[user_email]:
                mssg += " - ID del Issue: " + bad_issue["name"] + "\n"
                mssg += " - Descripción: " + bad_issue["summary"] + "\n"
            user = self.slack_client.get_user_by_email(user_email)
            self.slack_client.post_message_to_channel(
                channel=user['id'], message=mssg)

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
                    AND issue.stage != "Ready for Dev"
                    AND issue.stage != "Done"
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

        self.send_issues_qa_no_tester_report(bad_issues_by_user)

    def get_weekly_squads_bug_detail(self, squad_name):
        query = f"""
                         SELECT
                           processed.issue_summary,
                           processed.issue_name,
                           issue.project_name,
                           user.email
                         FROM (
                           SELECT
                             issue_summary,
                             issue_name,
                             MAX(updated_at) AS updated_at
                           FROM
                             `{self.dataset_id}.Issue`
                           WHERE
                             DATE(created_at)>=CURRENT_DATE("UTC-5:00")-7
                             AND issue_type = "Error"
                             AND project_name != "Support"
                           GROUP BY
                             issue_summary,
                             issue_name) AS processed,
                          `{self.dataset_id}.Issue` AS issue,
                          `{self.dataset_id}.User` AS user
                         WHERE
                           issue.issue_name = processed.issue_name
                           AND issue.updated_at = processed.updated_at
                           AND issue.project_name = "{squad_name}"
                           AND issue.assignee = user.account_id
                    """
        return self.client.query(query)

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
            "TPRO": Config.SLACK_SQUAD_TYBA_PROFESSIONAL,
            "Banner - Tyba Digital Colombia": Config.SLACK_SQUAD_BANNER,
            "Banner": Config.SLACK_SQUAD_BANNER,
            "Fury": Config.SLACK_SQUAD_FURY,
            "Parker": Config.SLACK_SQUAD_PARKER,
            "Robo": Config.SLACK_SQUAD_ROBO,
            "Groot": Config.SLACK_SQUAD_GROOT,
            "ROGERS": Config.SLACK_SQUAD_ROGERS,
            "Stark": Config.SLACK_SQUAD_STARK,
            "TybaCO": Config.SLACK_SQUAD_TYBACO,
        }
        query_job = self.client.query(query)
        for row in query_job:
            squad, avg_point, week_bugs = row[0], row[1], row[2]
            print(squad)
            mssg = f"""Hola :smile:, el rendimiento de {squad} en su última semana fue:\n
            - Promedio de story points/día del equipo: {avg_point}\n
            - Total de bugs en la semana: {week_bugs}\n"""

            bugs_percentage = self.weekly_percentage_bugs_report(squad=squad)
            mssg += bugs_percentage

            #self.slack_client.post_message_to_channel(
            #    channel=Config.SLACK_TEST_CHANNEL, message=mssg)

            self.slack_client.post_message_to_channel(
                channel=squad_params[squad], message=mssg)

            if(week_bugs > 0):
                bugs_detail = self.get_weekly_squads_bug_detail(squad)
                mssg = "------------------ \n"
                mssg += f"""Este es un resumen de los bugs en producción de la semana:\n"""
                for row in bugs_detail:
                    print(row)
                    summary, issue_id, project_name, assignee = row[0], row[1], row[2], row[3]
                    mssg += f"""{issue_id} - {summary}: \n
                    - Persona asignada: {assignee} \n \n"""
                #self.slack_client.post_message_to_channel(
                #   channel=Config.SLACK_TEST_CHANNEL, message=mssg)

                self.slack_client.post_message_to_channel(
                    channel=Config.SLACK_TEST_CHANNEL, message=mssg)

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
            mssg += ("\n" + self.weekly_percentage_bugs_report())
            self.slack_client.post_message_to_channel(
                channel=Config.SLACK_TEST_CHANNEL, message=mssg)
            #self.slack_client.post_message_to_channel(
            #    channel=Config.SLACK_SQUAD_TYBA_EOS, message=mssg)

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
                             `{self.dataset_id}.Issue`
                           WHERE
                             DATE(created_at)>=CURRENT_DATE("UTC-5:00")-14
                             AND issue_type = "Error"
                             AND project_name != "Support"
                           GROUP BY
                             issue_summary,
                             issue_name) AS processed,
                          `{self.dataset_id}.Issue` AS issue
                         WHERE
                           issue.issue_name = processed.issue_name
                           AND issue.updated_at = processed.updated_at
                           AND issue.project_name = '{squad_name}'
                    """
        return self.client.query(query)

    def weekly_percentage_bugs_report(self, squad=None):
        query = ""
        if squad == None:
            query = f"""
                    SELECT
                    *
                    FROM
                        `{self.dataset_id}.{Config.WEEK_TYBA_PERFORMANCE_TABLE}`
                    ORDER BY
                        index_date DESC
                    LIMIT 2;
                    """
        else:
            query = f"""
                    SELECT
                    *
                    FROM
                        `{self.dataset_id}.{Config.WEEK_SQUAD_PERFORMANCE_TABLE}`
                    WHERE
                        project_name = '{squad}'
                    ORDER BY
                        index_date DESC
                    LIMIT 2;
                    """
            
        dataframe = (self.client.query(query)
                    .result()
                    .to_dataframe(bqstorage_client=self.bqstorageclient)
                    )
        current_week_bugs = dataframe['week_bugs'].iloc[0]
        last_week_bugs = dataframe['week_bugs'].iloc[1]

        mssg = ""
        if last_week_bugs != 0 and current_week_bugs > last_week_bugs:
            increase_bugs_percentage = round(((current_week_bugs/last_week_bugs)-1),2)*100
            mssg = f"- Los bugs esta semana aumentaron: {increase_bugs_percentage}%"
        elif last_week_bugs != 0 and current_week_bugs < last_week_bugs:
            decrease_bugs_percentage = round((1-(current_week_bugs/last_week_bugs)),2)*100
            mssg = f"- Los bugs esta semana disminuyeron: {decrease_bugs_percentage}%"
        elif last_week_bugs != 0 and current_week_bugs == last_week_bugs: 
            mssg = f"- Los bugs esta semana se mantuvieron iguales"
        elif last_week_bugs == 0 and current_week_bugs > 0:
            mssg = f"- Esta semana hubo un aumento de {current_week_bugs} respecto a ningun bug la semana pasada"
        elif last_week_bugs == 0 and current_week_bugs == 0:
            mssg = f"- Genial!! :smile: dos semanas seguidas sin bugs, sigamos así :3."
        
        return mssg
####
  
    def process_warning_issues(self, query_job, dev_qa):
        issues_by_user = {}
        for row in query_job:
            user_email = row[2]
            print(row)
            if issues_by_user.get(user_email) == None:
                issues_by_user[user_email] = []
            issue_name = row[0]
            issue_summary = row[1]
            issue_sprint_start_date = row[4]
            

            days_in_key = f"days_in_{dev_qa}"
            days_in_val = row[3]

            now = datetime.now()
            sprint_start_date = datetime.strptime(issue_sprint_start_date, "%Y-%m-%d")
            sprint_days = abs((now - sprint_start_date).days)

            new_issue = {
                "name": issue_name,
                "summary": issue_summary,
                days_in_key: days_in_val,
                "sprint_days": sprint_days
            }
            issues_by_user[user_email].append(new_issue)
        
        return issues_by_user


    def warning_issues_qadev(self, week):
        query_qa = query_manager.warning_issues_qa()
        query_dev = query_manager.warning_issues_dev()

        query_qa_result = self.client.query(query_qa)
        query_dev_result = self.client.query(query_dev)

        warning_issues_qa = self.process_warning_issues(query_qa_result, "qa")
        warning_issues_dev = self.process_warning_issues(query_dev_result, "dev")

        if week == 1:
            for user_email in warning_issues_qa:
                issues_count = 0
                warning_issues_str = ""
                for warning_issue in warning_issues_qa[user_email]:
                    if warning_issue["days_in_qa"] == 4 and warning_issue["sprint_days"] <= 7:
                        issues_count+=1
                        warning_issues_str += " - ID del Issue: " + warning_issue["name"] + "\n"
                        warning_issues_str += " - Descripción: " + warning_issue["summary"] + "\n"
                if issues_count != 0:
                    warning_issues_start = "¡Hola! Noté que algunos issues asignados a ti llevan más de 3 días en QA. Aquí va el detalle:\n"
                    warning_issues_end = "¡Ánimo! Ve con toda la energía en este sprint :smile:"
                    warning_issues_mssg = warning_issues_start + warning_issues_str + warning_issues_end
                    #print(warning_issues_mssg)
                    user = self.slack_client.get_user_by_email(user_email)
                    self.slack_client.post_message_to_channel(channel=user['id'], message=warning_issues_mssg)

            for user_email in warning_issues_dev:
                issues_count = 0
                warning_issues_str = ""
                for warning_issue in warning_issues_dev[user_email]:
                    if warning_issue["days_in_dev"] == 4 and warning_issue["sprint_days"] <= 7:
                        issues_count+=1
                        warning_issues_str += " - ID del Issue: " + warning_issue["name"] + "\n"
                        warning_issues_str += " - Descripción: " + warning_issue["summary"] + "\n"
                if issues_count != 0:
                    warning_issues_start = "¡Hola! Noté que algunos issues asignados a ti llevan más de 3 días en DEV. Aquí va el detalle:\n"
                    warning_issues_end = "¡Ánimo! Ve con toda la energía en este sprint :smile:"
                    warning_issues_mssg = warning_issues_start + warning_issues_str + warning_issues_end
                    #print(warning_issues_mssg)
                    user = self.slack_client.get_user_by_email(user_email)
                    self.slack_client.post_message_to_channel(channel=user['id'], message=warning_issues_mssg)

        elif week == 2:
            for user_email in warning_issues_qa:
                issues_count = 0
                warning_issues_str = ""
                for warning_issue in warning_issues_qa[user_email]:
                    if warning_issue["days_in_qa"] >= 3 and warning_issue["sprint_days"] > 7:
                        issues_count+=1
                        warning_issues_str += " - ID del Issue: " + warning_issue["name"] + "\n"
                        warning_issues_str += " - Descripción: " + warning_issue["summary"] + "\n"
                if issues_count != 0:
                    warning_issues_start = "¡Hola! Noté que algunos issues asignados a ti llevan más de 3 días en QA. Aquí va el detalle:\n"
                    warning_issues_end = "¡Ánimo! Ve con toda la energía en este sprint :smile:"
                    warning_issues_mssg = warning_issues_start + warning_issues_str + warning_issues_end
                    #print(warning_issues_mssg)
                    user = self.slack_client.get_user_by_email(user_email)
                    self.slack_client.post_message_to_channel(channel=user['id'], message=warning_issues_mssg)
            
            for user_email in warning_issues_dev:
                issues_count = 0
                warning_issues_str = ""
                for warning_issue in warning_issues_dev[user_email]:
                    if warning_issue["days_in_dev"] >= 3 and warning_issue["sprint_days"] > 7:
                        issues_count+=1
                        warning_issues_str += " - ID del Issue: " + warning_issue["name"] + "\n"
                        warning_issues_str += " - Descripción: " + warning_issue["summary"] + "\n"
                if issues_count != 0:
                    warning_issues_start = "¡Hola! Noté que algunos issues asignados a ti llevan más de 3 días en DEV. Aquí va el detalle:\n"
                    warning_issues_end = "¡Ánimo! Ve con toda la energía en este sprint :smile:"
                    warning_issues_mssg = warning_issues_start + warning_issues_str + warning_issues_end
                    user = self.slack_client.get_user_by_email(user_email)
                    self.slack_client.post_message_to_channel(channel=user['id'], message=warning_issues_mssg)
        
    def warning_issues_ready_dev():
        query_ready_dev = query_manager.warning_issues_ready_dev()
        query_ready_dev_result = self.client.query(query_ready_dev)
        warning_issues_ready_dev = self.process_warning_issues(query_qa_result, "ready_dev")
        for user_email in warning_issues_ready_dev:
            issues_count = 0
            warning_issues_str = ""
            for warning_issue in warning_issues_ready_dev[user_email]:
                issues_count+=1
                warning_issues_str += " - ID del Issue: " + warning_issue["name"] + "\n"
                warning_issues_str += " - Descripción: " + warning_issue["summary"] + "\n"
            if issues_count != 0:
                warning_issues_start = "¡Hola! Noté que algunos issues asignados a ti llevan más de 7 días en 'Ready for DEV'. Aquí va el detalle:\n"
                warning_issues_end = "¡Ánimo! Ve con toda la energía en este sprint :smile:"
                warning_issues_mssg = warning_issues_start + warning_issues_str + warning_issues_end
                user = self.slack_client.get_user_by_email(user_email)
                self.slack_client.post_message_to_channel(channel=user['id'], message=warning_issues_mssg)
####

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

                    #last_day_date = now - dt2.timedelta(1)
                    # if created_date > last_day_date or updated_date > last_day_date:
                    #    records.append(parsed_issue)
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


def load_sprints(project_id, database_name):
    with TyBot(project_id, database_name) as db:

        query = f"""
                SELECT
                  DISTINCT name
                FROM
                  `{db.dataset_id}.{Config.SPRINT_TABLE}`
                """
        query_job = db.client.query(query)

        sprints_already_up = []
        for row in query_job:
            sprint_name = row[0]
            sprints_already_up.append(sprint_name)

        boards = get_all_boards()
        for board in boards:
            sprints = get_all_sprints_by_board(board["id"])
            records = []
            for sprint in sprints:
                parsed_sprint = {}
                if sprint.get("name") not in sprints_already_up and sprint.get("startDate"):
                    parsed_sprint["name"] = sprint.get("name")
                    parsed_sprint["start_date"] = str(dateutil.parser.parse(
                        sprint.get("startDate")))
                    parsed_sprint["end_date"] = str(dateutil.parser.parse(
                        sprint.get("endDate")))
                    print(parsed_sprint)
                    records.append(parsed_sprint)

            if records:
                db.insert_records("Sprint", records)
            print(
                "Inserted {} new sprints for board: {}".format(
                    len(records), board["id"]
                )
            )
