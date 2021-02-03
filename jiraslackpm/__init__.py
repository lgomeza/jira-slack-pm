import jira
import db
import slack

def main():
    slack_client = slack.SlackClient()
    user_response = slack_client.get_user_by_email("ivan@tyba.com.co")
    print(user_response)
    #slack_client.post_message_to_channel(channel="CJXPQCQKD", message="Hello tybers, I'm your new bot :)")

if __name__ == "__main__":
    main()