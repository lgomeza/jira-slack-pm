import jira
import db
import slack_connect
from config import Config

def main():
    
    tybot = db.TyBot(Config.BQ_PROJECT, Config.BQ_DATABASE)
    tybot.weekly_percentage_bugs_report()
    
    """
    slack_client = slack.SlackClient()
    user = slack_client.get_user_by_email("ivan@tyba.com.co")
    slack_client.post_message_to_channel(user['id'], "Hello I'm your bot")
    """


if __name__ == "__main__":
    main()
