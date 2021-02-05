import jira
import db
import slack
import pandas as pd
def main():
    tybot = db.TyBot("k-ren-295903", "jira")
    tybot.send_weekly_tyba_performance()

    """
    slack_client = slack.SlackClient()
    user = slack_client.get_user_by_email("ivan@tyba.com.co")
    slack_client.post_message_to_channel(user['id'], "Hello I'm your bot")
    """

if __name__ == "__main__":
    main()