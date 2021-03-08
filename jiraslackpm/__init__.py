import jira
import db
import slack_connect
from config import Config

def main():
    
    tybot = db.TyBot(Config.BQ_PROJECT, Config.BQ_DATABASE)
    tybot.warning_issues_qadev(week=1)

if __name__ == "__main__":
    main()
