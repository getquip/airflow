from custom_packages.notifications import slackbot

class CallbackNotifier:

  SLACK_CONN_ID='slack_bot'
  USERNAME = 'Airflow Alerts'

  @staticmethod
  def on_failure_callback(context):
    bot = SlackBot(CallbackNotifier.SLACK_CONN_ID,CallbackNotifier.USERNAME)
    bot.post_alert(context,title="Failed Task Alert")

  @staticmethod
  def on_retry_callback(context):
    bot = SlackBot(CallbackNotifier.SLACK_CONN_ID,CallbackNotifier.USERNAME)
    bot.post_warning(context,title="Retry Task Alert")