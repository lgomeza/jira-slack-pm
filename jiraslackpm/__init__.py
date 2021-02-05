import jira
import db
import slack
import pandas as pd
def main():
    tybot = db.TyBot("k-ren-295903", "jira")
    #tybot.send_bad_issues_report()
    query = """
            SELECT
            *
            FROM
            `k-ren-295903.jira.Weekly_metrics_squad` AS week_squad_pf
            WHERE
            week_squad_pf.index_date = CURRENT_DATE("UTC-5:00")-1
            and week_squad_pf.project_name = "Fury"
            ORDER BY
            week_squad_pf.avg_points DESC
            """
    query_job = tybot.client.query(query)
    """
    slack_client = slack.SlackClient()
    user = slack_client.get_user_by_email("ivan@tyba.com.co")
    slack_client.post_message_to_channel(user['id'], "Hello I'm your bot")
    """

if __name__ == "__main__":
    main()