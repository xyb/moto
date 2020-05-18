from __future__ import unicode_literals

from datetime import datetime

import boto
import boto3
import sure  # noqa

from nose.tools import assert_raises
from botocore.exceptions import ClientError
from moto import mock_iam
from moto.core import ACCOUNT_ID

MOCK_POLICY = """
{
  "Version": "2012-10-17",
  "Statement":
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::example_bucket"
    }
}
"""


@mock_iam
def test_create_group():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    with assert_raises(ClientError):
        conn.create_group(GroupName="my-group")


@mock_iam
def test_get_group():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    conn.get_group(GroupName="my-group")
    with assert_raises(ClientError):
        conn.get_group(GroupName="not-group")


@mock_iam
def test_get_group_current():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    result = conn.get_group(GroupName="my-group")

    assert result["Group"]["Path"] == "/"
    assert result["Group"]["GroupName"] == "my-group"
    assert isinstance(result["Group"]["CreateDate"], datetime)
    assert result["Group"]["GroupId"]
    assert result["Group"]["Arn"] == "arn:aws:iam::{}:group/my-group".format(ACCOUNT_ID)
    assert not result["Users"]

    # Make a group with a different path:
    other_group = conn.create_group(GroupName="my-other-group", Path="some/location")
    assert other_group["Group"]["Path"] == "some/location"
    assert other_group["Group"][
        "Arn"
    ] == "arn:aws:iam::{}:group/some/location/my-other-group".format(ACCOUNT_ID)


@mock_iam
def test_get_all_groups():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group1")
    conn.create_group(GroupName="my-group2")
    groups = conn.list_groups()["Groups"]
    groups.should.have.length_of(2)


@mock_iam
def test_add_user_to_group():
    conn = boto3.client("iam", region_name="us-east-1")
    with assert_raises(ClientError):
        conn.add_user_to_group(GroupName="my-group", UserName="my-user")
    conn.create_group(GroupName="my-group")
    with assert_raises(ClientError):
        conn.add_user_to_group(GroupName="my-group", UserName="my-user")
    conn.create_user(UserName="my-user")
    conn.add_user_to_group(GroupName="my-group", UserName="my-user")


@mock_iam
def test_remove_user_from_group():
    conn = boto3.client("iam", region_name="us-east-1")
    with assert_raises(ClientError):
        conn.remove_user_from_group(GroupName="my-group", UserName="my-user")
    conn.create_group(GroupName="my-group")
    conn.create_user(UserName="my-user")
    with assert_raises(ClientError):
        conn.remove_user_from_group(GroupName="my-group", UserName="my-user")
    conn.add_user_to_group(GroupName="my-group", UserName="my-user")
    conn.remove_user_from_group(GroupName="my-group", UserName="my-user")


@mock_iam
def test_get_groups_for_user():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group1")
    conn.create_group(GroupName="my-group2")
    conn.create_group(GroupName="other-group")
    conn.create_user(UserName="my-user")
    conn.add_user_to_group(GroupName="my-group1", UserName="my-user")
    conn.add_user_to_group(GroupName="my-group2", UserName="my-user")

    groups = conn.list_groups_for_user(UserName="my-user")["Groups"]
    groups.should.have.length_of(2)


@mock_iam
def test_put_group_policy():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    conn.put_group_policy(
        GroupName="my-group", PolicyName="my-policy", PolicyDocument=MOCK_POLICY
    )


@mock_iam
def test_attach_group_policies():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    conn.list_attached_group_policies(GroupName="my-group")[
        "AttachedPolicies"
    ].should.be.empty
    policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
    conn.list_attached_group_policies(GroupName="my-group")[
        "AttachedPolicies"
    ].should.be.empty
    conn.attach_group_policy(GroupName="my-group", PolicyArn=policy_arn)
    conn.list_attached_group_policies(GroupName="my-group")[
        "AttachedPolicies"
    ].should.equal(
        [{"PolicyName": "AmazonElasticMapReduceforEC2Role", "PolicyArn": policy_arn}]
    )

    conn.detach_group_policy(GroupName="my-group", PolicyArn=policy_arn)
    conn.list_attached_group_policies(GroupName="my-group")[
        "AttachedPolicies"
    ].should.be.empty


@mock_iam
def test_get_group_policy():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    with assert_raises(ClientError):
        conn.get_group_policy(GroupName="my-group", PolicyName="my-policy")

    conn.put_group_policy(
        GroupName="my-group", PolicyName="my-policy", PolicyDocument=MOCK_POLICY
    )
    conn.get_group_policy(GroupName="my-group", PolicyName="my-policy")


@mock_iam
def test_get_all_group_policies():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    policies = conn.list_group_policies(GroupName="my-group")["PolicyNames"]
    assert policies == []
    conn.put_group_policy(
        GroupName="my-group", PolicyName="my-policy", PolicyDocument=MOCK_POLICY
    )
    policies = conn.list_group_policies(GroupName="my-group")["PolicyNames"]
    assert policies == ["my-policy"]


@mock_iam()
def test_list_group_policies():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    conn.list_group_policies(GroupName="my-group")["PolicyNames"].should.be.empty
    conn.put_group_policy(
        GroupName="my-group", PolicyName="my-policy", PolicyDocument=MOCK_POLICY
    )
    conn.list_group_policies(GroupName="my-group")["PolicyNames"].should.equal(
        ["my-policy"]
    )


@mock_iam
def test_delete_group():
    conn = boto3.client("iam", region_name="us-east-1")
    conn.create_group(GroupName="my-group")
    groups = conn.list_groups()
    assert groups["Groups"][0]["GroupName"] == "my-group"
    assert len(groups["Groups"]) == 1
    conn.delete_group(GroupName="my-group")
    conn.list_groups()["Groups"].should.be.empty


@mock_iam
def test_delete_unknown_group():
    conn = boto3.client("iam", region_name="us-east-1")
    with assert_raises(ClientError) as err:
        conn.delete_group(GroupName="unknown-group")
    err.exception.response["Error"]["Code"].should.equal("NoSuchEntity")
    err.exception.response["Error"]["Message"].should.equal(
        "The group with name unknown-group cannot be found."
    )
