from __future__ import unicode_literals

import os
import json

import boto
import boto.iam
import boto.s3
import boto.s3.key
import boto.cloudformation
from boto.exception import BotoServerError
import boto3
from botocore.exceptions import ClientError
import sure  # noqa

# Ensure 'assert_raises' context manager support for Python 2.6
import tests.backport_assert_raises  # noqa
from nose.tools import assert_raises
from moto.core import ACCOUNT_ID

from moto import (
    mock_cloudformation,
    mock_s3,
    mock_route53,
    mock_iam,
)
from moto.cloudformation import cloudformation_backends

dummy_template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Stack 1",
    "Resources": {},
}

dummy_template2 = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Stack 2",
    "Resources": {},
}

# template with resource which has no delete attribute defined
dummy_template3 = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Stack 3",
    "Resources": {
        "VPC": {"Properties": {"CidrBlock": "192.168.0.0/16"}, "Type": "AWS::EC2::VPC"}
    },
}

dummy_template_json = json.dumps(dummy_template)
dummy_template_json2 = json.dumps(dummy_template2)
dummy_template_json3 = json.dumps(dummy_template3)


@mock_cloudformation
@mock_route53
def test_create_stack_hosted_zone_by_id():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    dummy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack 1",
        "Parameters": {},
        "Resources": {
            "Bar": {
                "Type": "AWS::Route53::HostedZone",
                "Properties": {"Name": "foo.bar.baz"},
            }
        },
    }
    dummy_template2 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack 2",
        "Parameters": {"ZoneId": {"Type": "String"}},
        "Resources": {
            "Foo": {
                "Properties": {"HostedZoneId": {"Ref": "ZoneId"}, "RecordSets": []},
                "Type": "AWS::Route53::RecordSetGroup",
            }
        },
    }
    conn.create_stack(
        StackName="test_stack", TemplateBody=json.dumps(dummy_template), Parameters=[],
    )
    r53_conn = boto3.client("route53", region_name="us-east-1")
    zone_id = r53_conn.list_hosted_zones()["HostedZones"][0]["Id"]
    conn.create_stack(
        StackName="test_stack",
        TemplateBody=json.dumps(dummy_template2),
        Parameters=[{"ParameterKey": "ZoneId", "ParameterValue": zone_id}],
    )

    stack = conn.describe_stacks()["Stacks"][0]
    assert conn.describe_stack_resources(StackName="test_stack")


@mock_cloudformation
def test_get_template_by_name():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    conn.create_stack(StackName="test_stack", TemplateBody=dummy_template_json)

    template = conn.get_template(StackName="test_stack")["TemplateBody"]
    template.should.equal(json.loads(dummy_template_json))


@mock_cloudformation
def test_delete_stack_by_id():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    stack_id = conn.create_stack(
        StackName="test_stack", TemplateBody=dummy_template_json
    )["StackId"]

    conn.describe_stacks()["Stacks"].should.have.length_of(1)
    conn.delete_stack(StackName=stack_id)
    conn.describe_stacks()["Stacks"].should.have.length_of(0)
    with assert_raises(ClientError):
        conn.describe_stacks(StackName="test_stack")

    conn.describe_stacks(StackName=stack_id)["Stacks"].should.have.length_of(1)


@mock_cloudformation
def test_delete_stack_with_resource_missing_delete_attr():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    conn.create_stack(StackName="test_stack", TemplateBody=dummy_template_json3)

    conn.describe_stacks()["Stacks"].should.have.length_of(1)
    conn.delete_stack(StackName="test_stack")
    conn.describe_stacks()["Stacks"].should.have.length_of(0)


@mock_cloudformation
def test_cloudformation_params_conditions_and_resources_are_distinct():
    dummy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack 1",
        "Conditions": {
            "FooEnabled": {"Fn::Equals": [{"Ref": "FooEnabled"}, "true"]},
            "FooDisabled": {
                "Fn::Not": [{"Fn::Equals": [{"Ref": "FooEnabled"}, "true"]}]
            },
        },
        "Parameters": {
            "FooEnabled": {"Type": "String", "AllowedValues": ["true", "false"]}
        },
        "Resources": {
            "Bar": {
                "Properties": {"CidrBlock": "192.168.0.0/16"},
                "Condition": "FooDisabled",
                "Type": "AWS::EC2::VPC",
            }
        },
    }
    dummy_template_json = json.dumps(dummy_template)
    cfn = boto3.client("cloudformation", region_name="us-east-1")
    cfn.create_stack(
        StackName="test_stack1",
        TemplateBody=dummy_template_json,
        Parameters=[{"ParameterKey": "FooEnabled", "ParameterValue": "true"}],
    )
    resources = cfn.list_stack_resources(StackName="test_stack1")[
        "StackResourceSummaries"
    ]
    assert not [
        resource for resource in resources if resource["LogicalResourceId"] == "Bar"
    ]


@mock_cloudformation
def test_update_stack_with_parameters():
    dummy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack",
        "Resources": {
            "VPC": {
                "Properties": {"CidrBlock": {"Ref": "Bar"}},
                "Type": "AWS::EC2::VPC",
            }
        },
        "Parameters": {"Bar": {"Type": "String"}},
    }
    dummy_template_json = json.dumps(dummy_template)
    conn = boto3.client("cloudformation", region_name="us-east-1")
    conn.create_stack(
        StackName="test_stack",
        TemplateBody=dummy_template_json,
        Parameters=[{"ParameterKey": "Bar", "ParameterValue": "192.168.0.0/16"}],
    )
    conn.update_stack(
        StackName="test_stack",
        TemplateBody=dummy_template_json,
        Parameters=[{"ParameterKey": "Bar", "ParameterValue": "192.168.0.1/16"}],
    )

    stack = conn.describe_stacks()["Stacks"][0]
    assert stack["Parameters"][0]["ParameterValue"] == "192.168.0.1/16"


@mock_cloudformation
def test_update_stack_replace_tags():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    conn.create_stack(
        StackName="test_stack",
        TemplateBody=dummy_template_json,
        Tags=[{"Key": "foo", "Value": "bar"}],
    )
    conn.update_stack(
        StackName="test_stack",
        TemplateBody=dummy_template_json,
        Tags=[{"Key": "foo", "Value": "baz"}],
    )

    stack = conn.describe_stacks()["Stacks"][0]
    stack["StackStatus"].should.equal("UPDATE_COMPLETE")
    # since there is one tag it doesn't come out as a list
    stack["Tags"][0]["Value"].should.equal("baz")


@mock_cloudformation
def test_update_stack_when_rolled_back():
    region_name = "us-east-1"
    conn = boto3.client("cloudformation", region_name=region_name)
    stack_id = conn.create_stack(
        StackName="test_stack", TemplateBody=dummy_template_json
    )["StackId"]

    cloudformation_backends[region_name].stacks[stack_id].status = "ROLLBACK_COMPLETE"

    with assert_raises(ClientError) as err:
        conn.update_stack(StackName="test_stack", TemplateBody=dummy_template_json)

    ex = err.exception.response
    ex["Error"]["Message"].should.match(
        r"is in ROLLBACK_COMPLETE state and can not be updated"
    )
    ex["Error"]["Code"].should.equal("ValidationError")
    ex["ResponseMetadata"]["HTTPStatusCode"].should.equal(400)


@mock_cloudformation
def test_create_stack_lambda_and_dynamodb():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    dummy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack Lambda Test 1",
        "Parameters": {},
        "Resources": {
            "func1": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"S3Bucket": "bucket_123", "S3Key": "key_123"},
                    "FunctionName": "func1",
                    "Handler": "handler.handler",
                    "Role": get_role_name(),
                    "Runtime": "python2.7",
                    "Description": "descr",
                    "MemorySize": 12345,
                },
            },
            "func1version": {
                "Type": "AWS::Lambda::Version",
                "Properties": {"FunctionName": {"Ref": "func1"}},
            },
            "tab1": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": "tab1",
                    "KeySchema": [{"AttributeName": "attr1", "KeyType": "HASH"}],
                    "AttributeDefinitions": [
                        {"AttributeName": "attr1", "AttributeType": "string"}
                    ],
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 10,
                        "WriteCapacityUnits": 10,
                    },
                },
            },
            "func1mapping": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": {"Ref": "func1"},
                    "EventSourceArn": "arn:aws:dynamodb:region:XXXXXX:table/tab1/stream/2000T00:00:00.000",
                    "StartingPosition": "0",
                    "BatchSize": 100,
                    "Enabled": True,
                },
            },
        },
    }
    validate_s3_before = os.environ.get("VALIDATE_LAMBDA_S3", "")
    try:
        os.environ["VALIDATE_LAMBDA_S3"] = "false"
        conn.create_stack(
            StackName="test_stack_lambda_1",
            TemplateBody=json.dumps(dummy_template),
            Parameters=[],
        )
    finally:
        os.environ["VALIDATE_LAMBDA_S3"] = validate_s3_before

    stack = conn.describe_stacks()["Stacks"][0]
    resources = conn.list_stack_resources(StackName="test_stack_lambda_1")[
        "StackResourceSummaries"
    ]
    assert len(resources) == 4


@mock_cloudformation
def test_create_stack_kinesis():
    conn = boto3.client("cloudformation", region_name="us-east-1")
    dummy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack Kinesis Test 1",
        "Parameters": {},
        "Resources": {
            "stream1": {
                "Type": "AWS::Kinesis::Stream",
                "Properties": {"Name": "stream1", "ShardCount": 2},
            }
        },
    }
    conn.create_stack(
        StackName="test_stack_kinesis_1",
        TemplateBody=json.dumps(dummy_template),
        Parameters=[],
    )

    stack = conn.describe_stacks()["Stacks"][0]
    resources = conn.list_stack_resources(StackName="test_stack_kinesis_1")[
        "StackResourceSummaries"
    ]
    assert len(resources) == 1


def get_role_name():
    with mock_iam():
        iam = boto3.client("iam")
        role = iam.create_role(
            AssumeRolePolicyDocument="{}",
            RoleName="my-role",
        )["Role"]["Arn"]
        return role
