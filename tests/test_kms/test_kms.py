# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import base64
import re

import boto.kms
import boto3
import six
import sure  # noqa
from boto.exception import JSONResponseError
from boto.kms.exceptions import AlreadyExistsException, NotFoundException
from botocore.exceptions import ClientError
from nose.tools import assert_raises
from parameterized import parameterized
from moto.core.exceptions import JsonRESTError
from moto.kms.models import KmsBackend
from moto.kms.exceptions import NotFoundException as MotoNotFoundException
from moto import mock_kms

PLAINTEXT_VECTORS = (
    (b"some encodeable plaintext",),
    (b"some unencodeable plaintext \xec\x8a\xcf\xb6r\xe9\xb5\xeb\xff\xa23\x16",),
    ("some unicode characters ø˚∆øˆˆ∆ßçøˆˆçßøˆ¨¥",),
)


def _get_encoded_value(plaintext):
    if isinstance(plaintext, six.binary_type):
        return plaintext

    return plaintext.encode("utf-8")


@mock_kms
def test_describe_key_via_alias():
    conn = boto3.client("kms", region_name="us-west-2")
    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    conn.create_alias(
        AliasName="alias/my-key-alias", TargetKeyId=key["KeyMetadata"]["KeyId"]
    )

    alias_key = conn.describe_key(KeyId="alias/my-key-alias")
    alias_key["KeyMetadata"]["Description"].should.equal("my key")
    alias_key["KeyMetadata"]["KeyUsage"].should.equal("ENCRYPT_DECRYPT")
    alias_key["KeyMetadata"]["Arn"].should.equal(key["KeyMetadata"]["Arn"])


@mock_kms
def test_describe_key_via_arn():
    conn = boto3.client("kms", region_name="us-west-2")
    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    arn = key["KeyMetadata"]["Arn"]

    the_key = conn.describe_key(KeyId=arn)
    the_key["KeyMetadata"]["Description"].should.equal("my key")
    the_key["KeyMetadata"]["KeyUsage"].should.equal("ENCRYPT_DECRYPT")
    the_key["KeyMetadata"]["KeyId"].should.equal(key["KeyMetadata"]["KeyId"])


@mock_kms
def test_describe_missing_key():
    conn = boto3.client("kms", region_name="us-west-2")
    conn.describe_key.when.called_with(KeyId="not-a-key").should.throw(ClientError)


@mock_kms
def test_list_keys():
    conn = boto3.client("kms", region_name="us-west-2")

    conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    conn.create_key(
        Policy="my policy", Description="my key2", KeyUsage="ENCRYPT_DECRYPT"
    )

    keys = conn.list_keys()
    keys["Keys"].should.have.length_of(2)


@mock_kms
def test_enable_key_rotation():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    conn.enable_key_rotation(KeyId=key_id)

    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(True)


@mock_kms
def test_enable_key_rotation_via_arn():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["Arn"]

    conn.enable_key_rotation(KeyId=key_id)

    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(True)


@mock_kms
def test_enable_key_rotation_with_alias_name_should_fail():
    conn = boto3.client("kms", region_name="us-west-2")
    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    conn.create_alias(
        AliasName="alias/my-key-alias", TargetKeyId=key["KeyMetadata"]["KeyId"]
    )

    alias_key = conn.describe_key(KeyId="alias/my-key-alias")
    alias_key["KeyMetadata"]["Arn"].should.equal(key["KeyMetadata"]["Arn"])

    conn.enable_key_rotation.when.called_with(KeyId="alias/my-alias").should.throw(
        ClientError
    )


@mock_kms
def test_disable_key_rotation():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    conn.enable_key_rotation(KeyId=key_id)
    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(True)

    conn.disable_key_rotation(KeyId=key_id)
    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(False)


@mock_kms
def test_disable_key_rotation_with_missing_key():
    conn = boto3.client("kms", region_name="us-west-2")
    conn.disable_key_rotation.when.called_with(KeyId="not-a-key").should.throw(
        ClientError
    )


@mock_kms
def test_get_key_rotation_status_with_missing_key():
    conn = boto3.client("kms", region_name="us-west-2")
    conn.get_key_rotation_status.when.called_with(KeyId="not-a-key").should.throw(
        ClientError
    )


@mock_kms
def test_get_key_rotation_status():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(False)


@mock_kms
def test_create_key_defaults_key_rotation():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    conn.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"].should.equal(False)


@mock_kms
def test_get_key_policy():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    policy = conn.get_key_policy(KeyId=key_id, PolicyName="default")
    policy["Policy"].should.equal("my policy")


@mock_kms
def test_get_key_policy_via_arn():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    policy = conn.get_key_policy(KeyId=key["KeyMetadata"]["Arn"], PolicyName="default")

    policy["Policy"].should.equal("my policy")


@mock_kms
def test_put_key_policy():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    conn.put_key_policy(KeyId=key_id, PolicyName="default", Policy="new policy")
    policy = conn.get_key_policy(KeyId=key_id, PolicyName="default")
    policy["Policy"].should.equal("new policy")


@mock_kms
def test_put_key_policy_via_arn():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["Arn"]

    conn.put_key_policy(KeyId=key_id, PolicyName="default", Policy="new policy")
    policy = conn.get_key_policy(KeyId=key_id, PolicyName="default")
    policy["Policy"].should.equal("new policy")


@mock_kms
def test_put_key_policy_via_alias_should_not_update():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    conn.create_alias(
        AliasName="alias/my-key-alias", TargetKeyId=key["KeyMetadata"]["KeyId"]
    )

    conn.put_key_policy.when.called_with(
        KeyId="alias/my-key-alias", PolicyName="default", Policy="new policy"
    ).should.throw(ClientError)

    policy = conn.get_key_policy(KeyId=key["KeyMetadata"]["KeyId"], PolicyName="default")
    policy["Policy"].should.equal("my policy")


@mock_kms
def test_put_key_policy():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    conn.put_key_policy(KeyId=key["KeyMetadata"]["Arn"], PolicyName="default", Policy="new policy")

    policy = conn.get_key_policy(KeyId=key["KeyMetadata"]["KeyId"], PolicyName="default")
    policy["Policy"].should.equal("new policy")


@mock_kms
def test_list_key_policies():
    conn = boto3.client("kms", region_name="us-west-2")

    key = conn.create_key(
        Policy="my policy", Description="my key1", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    policies = conn.list_key_policies(KeyId=key_id)
    policies["PolicyNames"].should.equal(["default"])


@mock_kms
def test__create_alias__returns_none_if_correct():
    conn = boto3.client("kms", region_name="us-west-2")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    resp = conn.create_alias(AliasName="alias/my-alias", TargetKeyId=key_id)
    resp.pop('ResponseMetadata')
    resp.should.equal({})


@mock_kms
def test__create_alias__raises_if_reserved_alias():
    conn = boto3.client("kms", region_name="us-west-2")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    reserved_aliases = [
        "alias/aws/ebs",
        "alias/aws/s3",
        "alias/aws/redshift",
        "alias/aws/rds",
    ]

    for alias_name in reserved_aliases:
        with assert_raises(ClientError) as err:
            conn.create_alias(AliasName=alias_name, TargetKeyId=key_id)

        ex = err.exception
        ex.response['Error']['Code'].should.equal("NotAuthorizedException")
        ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__create_alias__can_create_multiple_aliases_for_same_key_id():
    conn = boto3.client("kms", region_name="us-west-2")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    conn.create_alias(AliasName="alias/my-alias3", TargetKeyId=key_id)
    conn.create_alias(AliasName="alias/my-alias4", TargetKeyId=key_id)
    conn.create_alias(AliasName="alias/my-alias5", TargetKeyId=key_id)


@mock_kms
def test__create_alias__raises_if_wrong_prefix():
    conn = boto3.client("kms", region_name="us-west-2")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    with assert_raises(ClientError) as err:
        conn.create_alias(AliasName="wrongprefix/my-alias", TargetKeyId=key_id)

    ex = err.exception
    ex.response['Error']['Message'].should.equal("Invalid identifier")
    ex.response['Error']['Code'].should.equal("ValidationException")
    ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__create_alias__raises_if_duplicate():
    region = "us-west-2"
    conn = boto3.client("kms", region_name=region)
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]
    alias = "alias/my-alias"

    conn.create_alias(AliasName=alias, TargetKeyId=key_id)

    with assert_raises(ClientError) as err:
        conn.create_alias(AliasName=alias, TargetKeyId=key_id)

    ex = err.exception
    ex.response['Error']['Message'].should.match(
        r"An alias with the name arn:aws:kms:{region}:\d{{12}}:{alias} already exists".format(
            **locals()
        )
    )
    ex.response['Error']['Code'].should.equal("AlreadyExistsException")

    ex.response['message'].should.match(
        r"An alias with the name arn:aws:kms:{region}:\d{{12}}:{alias} already exists".format(
            **locals()
        )
    )
    ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__create_alias__raises_if_alias_has_restricted_characters():
    conn = boto3.client("kms", region_name="us-west-1")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    alias_names_with_restricted_characters = [
        "alias/my-alias!",
        "alias/my-alias$",
        "alias/my-alias@",
    ]

    for alias_name in alias_names_with_restricted_characters:
        with assert_raises(ClientError) as err:
            conn.create_alias(AliasName=alias_name, TargetKeyId=key_id)
        ex = err.exception
        ex.response['Error']['Message'].should.contain(
            "1 validation error detected: Value '{alias_name}' at 'aliasName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9:/_-]+$".format(
                **locals()
            )
        )
        ex.response['Error']['Code'].should.equal("ValidationException")
        ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)



@mock_kms
def test__create_alias__raises_if_alias_has_colon_character():
    # For some reason, colons are not accepted for an alias, even though they
    # are accepted by regex ^[a-zA-Z0-9:/_-]+$
    conn = boto3.client("kms", region_name="us-west-1")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    alias_names_with_restricted_characters = ["alias/my:alias"]

    for alias_name in alias_names_with_restricted_characters:
        with assert_raises(ClientError) as err:
            conn.create_alias(AliasName=alias_name, TargetKeyId=key_id)
        ex = err.exception
        ex.response['Error']['Code'].should.equal("ValidationException")
        ex.response['Error']['Message'].should.equal(
            "{alias_name} contains invalid characters for an alias".format(**locals())
        )
        ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@parameterized((("alias/my-alias_/",), ("alias/my_alias-/",)))
@mock_kms
def test__create_alias__accepted_characters(alias_name):
    conn = boto3.client("kms", region_name="us-west-1")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]

    conn.create_alias(AliasName=alias_name, TargetKeyId=key_id)


@mock_kms
def test__create_alias__raises_if_target_key_id_is_existing_alias():
    conn = boto3.client("kms", region_name="us-west-1")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]
    alias = "alias/my-alias"

    conn.create_alias(AliasName=alias, TargetKeyId=key_id)

    with assert_raises(ClientError) as err:
        conn.create_alias(AliasName=alias, TargetKeyId=alias)

    ex = err.exception
    ex.response['Error']['Code'].should.equal("ValidationException")
    ex.response['Error']['Message'].should.equal(
        "Aliases must refer to keys. Not aliases"
    )
    ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__delete_alias():
    conn = boto3.client("kms", region_name="us-west-1")
    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]
    alias = "alias/my-alias"

    # added another alias here to make sure that the deletion of the alias can
    # be done when there are multiple existing aliases.
    another_create_resp = conn.create_key()
    another_key_id = create_resp["KeyMetadata"]["KeyId"]
    another_alias = "alias/another-alias"

    conn.create_alias(AliasName=alias, TargetKeyId=key_id)
    conn.create_alias(AliasName=another_alias, TargetKeyId=another_key_id)

    resp = conn.delete_alias(AliasName=alias)

    resp.pop('ResponseMetadata')
    resp.should.equal({})

    # we can create the alias again, since it has been deleted
    conn.create_alias(AliasName=alias, TargetKeyId=key_id)


@mock_kms
def test__delete_alias__raises_if_wrong_prefix():
    conn = boto3.client("kms", region_name="us-west-1")

    with assert_raises(ClientError) as err:
        conn.delete_alias(AliasName="wrongprefix/my-alias")

    ex = err.exception
    ex.response['Error']['Code'].should.equal("ValidationException")
    ex.response['Error']['Message'].should.equal("Invalid identifier")
    ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__delete_alias__raises_if_alias_is_not_found():
    region = "us-west-2"
    conn = boto3.client("kms", region_name=region)
    alias_name = "alias/unexisting-alias"

    with assert_raises(ClientError) as err:
        conn.delete_alias(AliasName=alias_name)

    expected_message_match = r"Alias arn:aws:kms:{region}:[0-9]{{12}}:{alias_name} is not found.".format(
        region=region, alias_name=alias_name
    )
    ex = err.exception
    ex.response['Error']['Code'].should.equal("NotFoundException")
    ex.response['Error']['Message'].should.match(expected_message_match)
    ex.response['ResponseMetadata']['HTTPStatusCode'].should.equal(400)


@mock_kms
def test__list_aliases():
    region = "eu-west-1"
    conn = boto3.client("kms", region_name=region)

    create_resp = conn.create_key()
    key_id = create_resp["KeyMetadata"]["KeyId"]
    conn.create_alias(AliasName="alias/my-alias1", TargetKeyId=key_id)
    conn.create_alias(AliasName="alias/my-alias2", TargetKeyId=key_id)
    conn.create_alias(AliasName="alias/my-alias3", TargetKeyId=key_id)

    resp = conn.list_aliases()

    resp["Truncated"].should.be.false
    aliases = resp["Aliases"]

    def has_correct_arn(alias_obj):
        alias_name = alias_obj["AliasName"]
        alias_arn = alias_obj["AliasArn"]
        return re.match(
            r"arn:aws:kms:{region}:\d{{12}}:{alias_name}".format(
                region=region, alias_name=alias_name
            ),
            alias_arn,
        )

    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/aws/ebs" == alias["AliasName"]
        ]
    ).should.equal(1)
    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/aws/rds" == alias["AliasName"]
        ]
    ).should.equal(1)
    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/aws/redshift" == alias["AliasName"]
        ]
    ).should.equal(1)
    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/aws/s3" == alias["AliasName"]
        ]
    ).should.equal(1)

    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/my-alias1" == alias["AliasName"]
        ]
    ).should.equal(1)
    len(
        [
            alias
            for alias in aliases
            if has_correct_arn(alias) and "alias/my-alias2" == alias["AliasName"]
        ]
    ).should.equal(1)

    len(
        [
            alias
            for alias in aliases
            if "TargetKeyId" in alias and key_id == alias["TargetKeyId"]
        ]
    ).should.equal(3)

    len(aliases).should.equal(7)


@mock_kms
def test__assert_default_policy():
    from moto.kms.responses import _assert_default_policy

    _assert_default_policy.when.called_with("not-default").should.throw(
        MotoNotFoundException
    )
    _assert_default_policy.when.called_with("default").should_not.throw(
        MotoNotFoundException
    )


if six.PY2:
    sort = sorted
else:
    sort = lambda l: sorted(l, key=lambda d: d.keys())


@mock_kms
def test_key_tag_on_create_key_happy():
    client = boto3.client("kms", region_name="us-east-1")

    tags = [
        {"TagKey": "key1", "TagValue": "value1"},
        {"TagKey": "key2", "TagValue": "value2"},
    ]
    key = client.create_key(Description="test-key-tagging", Tags=tags)
    key_id = key["KeyMetadata"]["KeyId"]

    result = client.list_resource_tags(KeyId=key_id)
    actual = result.get("Tags", [])
    assert sort(tags) == sort(actual)

    client.untag_resource(KeyId=key_id, TagKeys=["key1"])

    actual = client.list_resource_tags(KeyId=key_id).get("Tags", [])
    expected = [{"TagKey": "key2", "TagValue": "value2"}]
    assert sort(expected) == sort(actual)


@mock_kms
def test_key_tag_added_happy():
    client = boto3.client("kms", region_name="us-east-1")

    key = client.create_key(Description="test-key-tagging")
    key_id = key["KeyMetadata"]["KeyId"]
    tags = [
        {"TagKey": "key1", "TagValue": "value1"},
        {"TagKey": "key2", "TagValue": "value2"},
    ]
    client.tag_resource(KeyId=key_id, Tags=tags)

    result = client.list_resource_tags(KeyId=key_id)
    actual = result.get("Tags", [])
    assert sort(tags) == sort(actual)

    client.untag_resource(KeyId=key_id, TagKeys=["key1"])

    actual = client.list_resource_tags(KeyId=key_id).get("Tags", [])
    expected = [{"TagKey": "key2", "TagValue": "value2"}]
    assert sort(expected) == sort(actual)


@mock_kms
def test_key_tagging_sad():
    b = KmsBackend()

    try:
        b.tag_resource("unknown", [])
        raise "tag_resource should fail if KeyId is not known"
    except JsonRESTError:
        pass

    try:
        b.untag_resource("unknown", [])
        raise "untag_resource should fail if KeyId is not known"
    except JsonRESTError:
        pass

    try:
        b.list_resource_tags("unknown")
        raise "list_resource_tags should fail if KeyId is not known"
    except JsonRESTError:
        pass
