import jira
import db
import slack_connect


def main():
    """
    tybot = db.TyBot("k-ren-295903", "jira")
    tybot.send_issues_qa_no_tester_report({})
    """
    db.load_sprints("k-ren-295903", "jira")
    """
    users = jira.get_all_users()
    for user in users:
        issues = jira.get_all_issues_by_user(user["accountId"])
        for issue in issues:
            parsed_issue = jira.get_info_from_issue(issue)
            print(parsed_issue)
    """
    """
    slack_client = slack.SlackClient()
    user = slack_client.get_user_by_email("ivan@tyba.com.co")
    slack_client.post_message_to_channel(user['id'], "Hello I'm your bot")
    """


if __name__ == "__main__":
    main()
