from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

class SlackBot():

    def __init__(self, conn_id, username):
        self.conn_id = conn_id
        self.username = username
        self.token = BaseHook.get_connection(self.conn_id).password

    def post_message(self, context, message):
        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id=self.conn_id,
            webhook_token=self.token,
            message=message,
            username=self.username)
        return slack_alert.execute(context=context)

    def __format_message(self, context, **kwargs):
        local_dt = context.get('execution_date').astimezone()
        options = {
            'icon' : ':large_blue_circle:',
            'title': '[not provided]'
        }
        options.update(kwargs)
        slack_msg = """
            ==================================================-
            {icon} *{title}*
            ==================================================
            *Task*: {task}
            *Dag*: {dag}
            *Execution Date:*: {execution_date}
            *Log Url*: {log_url}
            """.format(
                icon=options['icon'],
                title=options['title'],
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                execution_date=local_dt.strftime('%Y%m%d %H:00:00'),
                log_url=context.get('task_instance').log_url
            )
        return slack_msg

    def post_alert(self, context, **kwargs):
        kwargs['icon'] = ':red_circle:'
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)

    def post_info(self, context, **kwargs):
        kwargs['icon'] = ':large_green_circle:'
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)

    def post_warning(self, context, **kwargs):
        kwargs['icon'] = ':large_orange_circle:'
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)