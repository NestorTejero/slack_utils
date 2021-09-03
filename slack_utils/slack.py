import logging
import time
import sys

import arrow
from slackclient import SlackClient

log = logging.getLogger('slack_maintenance')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

retry_after_text = ['Retry-After', 'retry-after']


def timestamp_x_days_ago(days, initial_date=None):
    """Given an Arrow date, convert into timestamp (seconds since 1/1/1970 00:00:00)"""

    if not initial_date:
        initial_date = arrow.now()
    return initial_date.shift(days=-days).timestamp()


class Slack(object):
    """Class to perform calls to the Slack API.

    It needs a legacy token to work. You can get it from https://api.slack.com/custom-integrations/legacy-tokens
    since it is personal, it will allow to delete those messages that the user requesting the token is allowed to delete
    """
    _instance = None

    @staticmethod
    def get_instance(slack_token):
        if Slack._instance is None:
            Slack._instance = Slack(slack_token)
        return Slack._instance

    def __init__(self, slack_token):
        """
        :param slack_token: string with the legacy token given by Slack.
        """
        self.sc = SlackClient(slack_token)

    def list_of_channels(self):
        """Return set of tuples with id and name of the existing channels."""

        channels_list = self.sc.api_call('conversations.list')

        return [(c['id'], c['name']) for c in channels_list['channels']]

    def map_channels_to_their_id(self, rules):
        """Given a dict with rules, add the id of channels to each block."""

        list_of_channels_and_id = self.list_of_channels()

        mapping_channels_and_id = dict()
        for (id_channel, channel_name) in list_of_channels_and_id:
            channel_name = channel_name.replace('#', '')
            mapping_channels_and_id[channel_name] = id_channel

        for rule in rules:
            rule['channel'] = rule['channel'].replace('#', '')
            if rule['channel'] not in mapping_channels_and_id:
                log.error('The channel %s does not exist or it is not accessible.' % rule['channel'])
                raise Exception('The channel %s does not exist or it is not accessible.' % rule['channel'])
            rule['id'] = mapping_channels_and_id[rule['channel']]

        return rules

    def load_messages_from_channel(self, channel_id, min_days_old, message_type=None, message_subtype=None):
        """Load all the messages from a given channel, according to the given filters.

        :param channel_id: id of the channel from which we get the messages
        :param min_days_old: min amount of days of the message
        :param message_type: whether it is a message, file...
        :param message_subtype: whether it is a bot, ...

        :return: list of ids (timestamps) of the messages
        """

        # If number of days is set to 0, we keep it
        # we just set a default value if it was not given
        if min_days_old is None:
            # min_days_old = settings.SLACK['min_days_old']
            min_days_old = 15

        minimum_timestamp = timestamp_x_days_ago(min_days_old)

        ts_messages = set()
        oldest = 0

        iterations = 0
        # Iterate loading messages until there are no more to load
        # It assumes messages being provided from new to old (newest first)
        # Even so, limit the iterations to prevent an infinite loop
        while iterations < 10:
            # Docs of the call: https://api.slack.com/methods/conversations.history
            res = self.sc.api_call('conversations.history', channel=channel_id, latest=minimum_timestamp,
                                   oldest=oldest, inclusive=False, count=1000)

            if not res['ok']:
                log.error('Error loading messages from channel %s: %s' % (channel_id, res['error']))
                raise Exception('Error loading messages from channel %s: %s' % (channel_id, res['error']))

            new_messages = res['messages']

            if message_type:
                new_messages = list(filter(lambda x: x['type'] == message_type, new_messages))

            if message_subtype:
                new_messages = list(filter(lambda x: x['subtype'] == message_subtype, new_messages))

            # Possible improvement: add filter by user sending the message (x['user'])

            # Save timestamp of the messages
            ts_new_messages = set([m['ts'] for m in new_messages])

            # If no messages are being loaded
            if not ts_new_messages or ts_new_messages.issubset(ts_messages):
                # No more messages. We stop iterating
                break

            # Add new timestamps. Use of set() to delete duplicates
            ts_messages = ts_messages.union(ts_new_messages)

            # It prepares the next iteration
            # As 'channels.history' returns the newer messages,
            # we set the oldest as the upper limit for the next iteration
            # and we substract an epsilon so it does not show as a result in the next iteration
            minimum_timestamp = str(float(min(ts_new_messages)) - 0.000001)

            iterations += 1

        return ts_messages

    def delete_channel_messages(self, channel_id, messages):
        """Delete the messages of the given channel.

        :param channel_id: id of the channel (not the name)
        :param messages: list of ids of the messages to be deleted

        :return: either the amount of messages to be removed or nothing
        """

        missing_messages = set(messages)
        number_of_deleted_messages = 0

        # We delete the messages one by one (the API does not allow to bulk delete)
        # When we get an error for too many requests,
        # we return the list of messages not deleted yet + num of seconds to wait
        for ts_message in messages:
            res = self.sc.api_call('chat.delete', channel=channel_id, ts=ts_message)
            if not res['ok']:
                headers = res.get('headers', {})
                if any(it in headers.keys() for it in retry_after_text):
                    retry_after = int(headers.get('Retry-After') or headers.get('retry-after') or 2)
                    log.info('%d deleted messages. We must wait %d second/s' % (number_of_deleted_messages, retry_after))
                    return missing_messages, retry_after
                else:
                    log.error('Error deleting the message %s from the channel %s: %s' % (ts_message, channel_id, res['error']))
                    pass
                    # raise Exception('Error deleting the message %s from the channel %s: %s' % (ts_message, channel_id, res['error']))
            missing_messages.remove(ts_message)
            number_of_deleted_messages += 1

        return None

    @staticmethod
    def delete_messages(slack_token, rules):
        """Perform deletion of messages on the given channels.

        Rules are given with a dictionary on the form:
        [{
            "channel": "#bla",
            "days": "30"
        }, {
            "channel": "#ble",
            "days": "60"
        }]

        :param slack_token: string with the Slack token
        :param rules: dictionary with rules over which channels to delete and how old days should be at least
        """

        sc = Slack.get_instance(slack_token=slack_token)
        rules_by_channel = sc.map_channels_to_their_id(rules)

        # Print number of messages by channel
        # ch = sc.list_of_channels()
        # for c, n in ch:
        #    r = sc.load_messages_from_channel(c, min_days_old=0)
        #    print(n, len(r))

        for rule in rules_by_channel:
            min_days_old = int(rule['days'])

            ts_messages = sc.load_messages_from_channel(channel_id=rule['id'], min_days_old=min_days_old)

            if not ts_messages:
                log.info("There are no messages to delete on channel %s." % rule['channel'])
                continue

            log.warning("Deleting %d messages from the channel #%s that are more than %d days old..."
                 % (len(ts_messages), rule['channel'], min_days_old))

            missing_messages = ts_messages
            while True:
                res = sc.delete_channel_messages(channel_id=rule['id'], messages=missing_messages)
                if not res:
                    # There are no more messages to delete
                    break

                missing_messages, delay = res
                time.sleep(delay)
                log.info("Waiting %d seconds" % delay)
