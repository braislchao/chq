"""AWS Lambda entry point for scheduled reports."""

from chq.config import load_config
from chq.runner import run


def lambda_handler(event, context):
    config = load_config()
    run(config)
    return {"statusCode": 200, "body": "Report sent"}
