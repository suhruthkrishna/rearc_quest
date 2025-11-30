from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources
)
from constructs import Construct
import os


class RearcQuestStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
#This is what we see in the s3 bucket since it is automated just called it RearcQuestBucket
        bucket = s3.Bucket(
            self,
            "RearcQuestBucket",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
#This is what is shown in SQS Queue region and if there are any additional messages here
        queue = sqs.Queue(
            self,
            "RearcQuestQueue",
            visibility_timeout=Duration.seconds(330)
        )
#Complete ingestion pipeline it first looks and works with the ingestion_handler that is loading of the data
        ingestion_lambda = _lambda.Function(
            self,
            "IngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="ingestion_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/ingestion"),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )

        bucket.grant_read_write(ingestion_lambda)
#For now this works once everyday or is called once a day
        events.Rule(
            self,
            "DailyIngestionSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(ingestion_lambda)]
        )
#Now that the ingestion is taken place the next would be to look over the analytics since that is the next step and since we already have analytics handler this is also similar way to call
        analytics_lambda = _lambda.Function(
            self,
            "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="analytics_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/analytics"),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )

        bucket.grant_read(analytics_lambda)
# Finally the collab of s3 and sqs notification which is mentioned in the task
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue)
        )


        analytics_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(queue, batch_size=1)
        )
