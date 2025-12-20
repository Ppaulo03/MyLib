from common.responses import success


def lambda_handler(event, context):
    return success({"status": "OK"})
