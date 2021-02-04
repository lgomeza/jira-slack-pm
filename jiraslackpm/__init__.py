import jira
import db
import slack

def main():
    bqclient = db.TyBot("k-ren-295903", "jira")
    bqclient.send_bad_issues_report()
    """
    slack_client = slack.SlackClient()
    user = slack_client.get_user_by_email("ivan@tyba.com.co")
    slack_client.post_message_to_channel(user['id'], "Hello I'm your bot")
    """

if __name__ == "__main__":
    main()