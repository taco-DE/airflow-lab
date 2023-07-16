from module.util.notifier import Notifier


def success_callback_slack_handler(**content):
    content["callback_type"] = "success"
    Notifier.send_msg(content)


def failure_callback_slack_handler(**content):
    content["callback_type"] = "failure"
    Notifier.send_msg(content)
