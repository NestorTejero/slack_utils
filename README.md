Repo to contain any utils that may be used in different projects of Naveler.


Sample usage of the Slack module `slackutils.slack` to remove messages:

````
from naveutils.slack import Slack

token = 'xoxp-...'  # to get from https://api.slack.com/custom-integrations/legacy-tokens
rules = [
    {'channel': '#gitlab', 'days': '5'},
    {'channel': '#trello', 'days': '15'}
]

Slack.delete_messages(token, rules)
````
