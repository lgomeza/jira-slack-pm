"""Bot config."""
from os import environ, path
from dotenv import load_dotenv

BASE_DIR = path.abspath(path.dirname(__file__))
load_dotenv(path.join(BASE_DIR, '.env'))


class Config:
    """Flask configuration variables."""

    # General Config
    SLACK_OAUTH_ACCESS_TOKEN = environ.get('SLACK_OAUTH_ACCESS_TOKEN')
    WEEK_DEVS_PERFORMANCE_TABLE= environ.get('WEEK_DEVS_PERFORMANCE_TABLE')
    WEEK_SQUAD_PERFORMANCE_TABLE = environ.get('WEEK_SQUAD_PERFORMANCE_TABLE')
    SLACK_SQUAD_TYBA_PROFESSIONAL = environ.get('SLACK_SQUAD_TYBA_PROFESSIONAL')
    SLACK_SQUAD_BANNER = environ.get('SLACK_SQUAD_BANNER')
    SLACK_SQUAD_FURY = environ.get('SLACK_SQUAD_FURY')
    SLACK_SQUAD_PARKER = environ.get('SLACK_SQUAD_PARKER')
