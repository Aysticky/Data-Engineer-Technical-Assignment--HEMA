STEP_FUNCTIONS_DEFINITION = {
    "Comment": "Retail sales data pipeline - daily batch workflow",
    "StartAt": "BronzeIngestion",
    "States": {
        "BronzeIngestion": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:hema-bronze-ingest",
            "Parameters": {
                "source_file.$": "$.source_file",
                "use_local": False
            },
            "ResultPath": "$.bronze_result",
            "Next": "SilverTransformation",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "FailureNotification",
                    "ResultPath": "$.error"
                }
            ],
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0
                }
            ]
        },
        "SilverTransformation": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "hema-silver-cleanse",
                "Arguments": {
                    "--enable-metrics": "true",
                    "--enable-spark-ui": "true"
                }
            },
            "ResultPath": "$.silver_result",
            "Next": "GoldParallel",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "FailureNotification",
                    "ResultPath": "$.error"
                }
            ],
            "Retry": [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0
                }
            ]
        },
        "GoldParallel": {
            "Type": "Parallel",
            "ResultPath": "$.gold_results",
            "Next": "SuccessNotification",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "FailureNotification",
                    "ResultPath": "$.error"
                }
            ],
            "Branches": [
                {
                    "StartAt": "GoldSales",
                    "States": {
                        "GoldSales": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "hema-gold-sales",
                                "Arguments": {
                                    "--enable-metrics": "true",
                                    "--enable-spark-ui": "true"
                                }
                            },
                            "End": True,
                            "Retry": [
                                {
                                    "ErrorEquals": ["States.TaskFailed"],
                                    "IntervalSeconds": 60,
                                    "MaxAttempts": 2,
                                    "BackoffRate": 2.0
                                }
                            ]
                        }
                    }
                },
                {
                    "StartAt": "GoldCustomer",
                    "States": {
                        "GoldCustomer": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "hema-gold-customer",
                                "Arguments": {
                                    "--enable-metrics": "true",
                                    "--enable-spark-ui": "true"
                                }
                            },
                            "End": True,
                            "Retry": [
                                {
                                    "ErrorEquals": ["States.TaskFailed"],
                                    "IntervalSeconds": 60,
                                    "MaxAttempts": 2,
                                    "BackoffRate": 2.0
                                }
                            ]
                        }
                    }
                }
            ]
        },
        "SuccessNotification": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:${AWS::Region}:${AWS::AccountId}:hema-pipeline-notifications",
                "Subject": "HEMA Retail Sales Pipeline - Success",
                "Message": {
                    "status": "success",
                    "execution_id.$": "$$.Execution.Name",
                    "timestamp.$": "$$.Execution.StartTime"
                }
            },
            "End": True
        },
        "FailureNotification": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "arn:aws:sns:${AWS::Region}:${AWS::AccountId}:hema-pipeline-notifications",
                "Subject": "HEMA Retail Sales Pipeline - Failure",
                "Message": {
                    "status": "failed",
                    "execution_id.$": "$$.Execution.Name",
                    "timestamp.$": "$$.Execution.StartTime",
                    "error.$": "$.error"
                }
            },
            "End": True
        }
    }
}
