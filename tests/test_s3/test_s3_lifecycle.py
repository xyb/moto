from __future__ import unicode_literals

import boto3

import sure  # noqa
from botocore.exceptions import ClientError
from datetime import datetime
from dateutil.tz import tzutc
from nose.tools import assert_raises

from moto import mock_s3


@mock_s3
def test_lifecycle_create():
    conn = boto3.client("s3", region_name="us-west-1")
    bucket = conn.create_bucket(
        Bucket="foobar", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    conn.put_bucket_lifecycle(
        Bucket="foobar",
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "myid",
                    "Prefix": "",
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        },
    )

    response = conn.get_bucket_lifecycle(Bucket="foobar")["Rules"]
    len(response).should.equal(1)
    lifecycle = response[0]
    lifecycle["ID"].should.equal("myid")
    lifecycle["Prefix"].should.equal("")
    lifecycle["Status"].should.equal("Enabled")
    list(lifecycle.get("Transition", [])).should.equal([])


@mock_s3
def test_lifecycle_with_filters():
    client = boto3.client("s3")
    client.create_bucket(
        Bucket="bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    # Create a lifecycle rule with a Filter (no tags):
    lfc = {
        "Rules": [
            {
                "Expiration": {"Days": 7},
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["Filter"]["Prefix"] == ""
    assert not result["Rules"][0]["Filter"].get("And")
    assert not result["Rules"][0]["Filter"].get("Tag")
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # Without any prefixes and an empty filter (this is by default a prefix for the whole bucket):
    lfc = {
        "Rules": [
            {
                "Expiration": {"Days": 7},
                "ID": "wholebucket",
                "Filter": {},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # If we remove the filter -- and don't specify a Prefix, then this is bad:
    lfc["Rules"][0].pop("Filter")
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"

    # With a tag:
    lfc["Rules"][0]["Filter"] = {"Tag": {"Key": "mytag", "Value": "mytagvalue"}}
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    with assert_raises(KeyError):
        assert result["Rules"][0]["Filter"]["Prefix"]
    assert not result["Rules"][0]["Filter"].get("And")
    assert result["Rules"][0]["Filter"]["Tag"]["Key"] == "mytag"
    assert result["Rules"][0]["Filter"]["Tag"]["Value"] == "mytagvalue"
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # With And (single tag):
    lfc["Rules"][0]["Filter"] = {
        "And": {
            "Prefix": "some/prefix",
            "Tags": [{"Key": "mytag", "Value": "mytagvalue"}],
        }
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert not result["Rules"][0]["Filter"].get("Prefix")
    assert result["Rules"][0]["Filter"]["And"]["Prefix"] == "some/prefix"
    assert len(result["Rules"][0]["Filter"]["And"]["Tags"]) == 1
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Key"] == "mytag"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Value"] == "mytagvalue"
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # With multiple And tags:
    lfc["Rules"][0]["Filter"]["And"] = {
        "Prefix": "some/prefix",
        "Tags": [
            {"Key": "mytag", "Value": "mytagvalue"},
            {"Key": "mytag2", "Value": "mytagvalue2"},
        ],
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert not result["Rules"][0]["Filter"].get("Prefix")
    assert result["Rules"][0]["Filter"]["And"]["Prefix"] == "some/prefix"
    assert len(result["Rules"][0]["Filter"]["And"]["Tags"]) == 2
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Key"] == "mytag"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Value"] == "mytagvalue"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][1]["Key"] == "mytag2"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][1]["Value"] == "mytagvalue2"
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # And filter without Prefix but multiple Tags:
    lfc["Rules"][0]["Filter"]["And"] = {
        "Tags": [
            {"Key": "mytag", "Value": "mytagvalue"},
            {"Key": "mytag2", "Value": "mytagvalue2"},
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    with assert_raises(KeyError):
        assert result["Rules"][0]["Filter"]["And"]["Prefix"]
    assert len(result["Rules"][0]["Filter"]["And"]["Tags"]) == 2
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Key"] == "mytag"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][0]["Value"] == "mytagvalue"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][1]["Key"] == "mytag2"
    assert result["Rules"][0]["Filter"]["And"]["Tags"][1]["Value"] == "mytagvalue2"
    with assert_raises(KeyError):
        assert result["Rules"][0]["Prefix"]

    # Can't have both filter and prefix:
    lfc["Rules"][0]["Prefix"] = ""
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"

    lfc["Rules"][0]["Prefix"] = "some/path"
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"

    # No filters -- just a prefix:
    del lfc["Rules"][0]["Filter"]
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert not result["Rules"][0].get("Filter")
    assert result["Rules"][0]["Prefix"] == "some/path"

    # Can't have Tag, Prefix, and And in a filter:
    del lfc["Rules"][0]["Prefix"]
    lfc["Rules"][0]["Filter"] = {
        "Prefix": "some/prefix",
        "Tag": {"Key": "mytag", "Value": "mytagvalue"},
    }
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"

    lfc["Rules"][0]["Filter"] = {
        "Tag": {"Key": "mytag", "Value": "mytagvalue"},
        "And": {
            "Prefix": "some/prefix",
            "Tags": [
                {"Key": "mytag", "Value": "mytagvalue"},
                {"Key": "mytag2", "Value": "mytagvalue2"},
            ],
        },
    }
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"

    # Make sure multiple rules work:
    lfc = {
        "Rules": [
            {
                "Expiration": {"Days": 7},
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            },
            {
                "Expiration": {"Days": 10},
                "ID": "Tags",
                "Filter": {"Tag": {"Key": "somekey", "Value": "somevalue"}},
                "Status": "Enabled",
            },
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")["Rules"]
    assert len(result) == 2
    assert result[0]["ID"] == "wholebucket"
    assert result[1]["ID"] == "Tags"


@mock_s3
def test_lifecycle_with_eodm():
    client = boto3.client("s3")
    client.create_bucket(
        Bucket="bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    lfc = {
        "Rules": [
            {
                "Expiration": {"ExpiredObjectDeleteMarker": True},
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["Expiration"]["ExpiredObjectDeleteMarker"]

    # Set to False:
    lfc["Rules"][0]["Expiration"]["ExpiredObjectDeleteMarker"] = False
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert not result["Rules"][0]["Expiration"]["ExpiredObjectDeleteMarker"]

    # With failure:
    lfc["Rules"][0]["Expiration"]["Days"] = 7
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"
    del lfc["Rules"][0]["Expiration"]["Days"]

    lfc["Rules"][0]["Expiration"]["Date"] = datetime(2015, 1, 1)
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"


@mock_s3
def test_lifecycle_with_nve():
    client = boto3.client("s3")
    client.create_bucket(
        Bucket="bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    lfc = {
        "Rules": [
            {
                "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["NoncurrentVersionExpiration"]["NoncurrentDays"] == 30

    # Change NoncurrentDays:
    lfc["Rules"][0]["NoncurrentVersionExpiration"]["NoncurrentDays"] = 10
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["NoncurrentVersionExpiration"]["NoncurrentDays"] == 10

    # TODO: Add test for failures due to missing children


@mock_s3
def test_lifecycle_with_nvt():
    client = boto3.client("s3")
    client.create_bucket(
        Bucket="bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    lfc = {
        "Rules": [
            {
                "NoncurrentVersionTransitions": [
                    {"NoncurrentDays": 30, "StorageClass": "ONEZONE_IA"}
                ],
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["NoncurrentVersionTransitions"][0]["NoncurrentDays"] == 30
    assert (
        result["Rules"][0]["NoncurrentVersionTransitions"][0]["StorageClass"]
        == "ONEZONE_IA"
    )

    # Change NoncurrentDays:
    lfc["Rules"][0]["NoncurrentVersionTransitions"][0]["NoncurrentDays"] = 10
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert result["Rules"][0]["NoncurrentVersionTransitions"][0]["NoncurrentDays"] == 10

    # Change StorageClass:
    lfc["Rules"][0]["NoncurrentVersionTransitions"][0]["StorageClass"] = "GLACIER"
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert (
        result["Rules"][0]["NoncurrentVersionTransitions"][0]["StorageClass"]
        == "GLACIER"
    )

    # With failures for missing children:
    del lfc["Rules"][0]["NoncurrentVersionTransitions"][0]["NoncurrentDays"]
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"
    lfc["Rules"][0]["NoncurrentVersionTransitions"][0]["NoncurrentDays"] = 30

    del lfc["Rules"][0]["NoncurrentVersionTransitions"][0]["StorageClass"]
    with assert_raises(ClientError) as err:
        client.put_bucket_lifecycle_configuration(
            Bucket="bucket", LifecycleConfiguration=lfc
        )
    assert err.exception.response["Error"]["Code"] == "MalformedXML"


@mock_s3
def test_lifecycle_with_aimu():
    client = boto3.client("s3")
    client.create_bucket(
        Bucket="bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    lfc = {
        "Rules": [
            {
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
                "ID": "wholebucket",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
            }
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert (
        result["Rules"][0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] == 7
    )

    # Change DaysAfterInitiation:
    lfc["Rules"][0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] = 30
    client.put_bucket_lifecycle_configuration(
        Bucket="bucket", LifecycleConfiguration=lfc
    )
    result = client.get_bucket_lifecycle_configuration(Bucket="bucket")
    assert len(result["Rules"]) == 1
    assert (
        result["Rules"][0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"]
        == 30
    )

    # TODO: Add test for failures due to missing children


@mock_s3
def test_lifecycle_with_glacier_transition():
    conn = boto3.client("s3", region_name="us-west-1")
    bucket = conn.create_bucket(
        Bucket="foobar", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    conn.put_bucket_lifecycle_configuration(
        Bucket="foobar",
        LifecycleConfiguration={
            "Rules": [
                {
                    "Transitions": [{"Days": 30, "StorageClass": "GLACIER",},],
                    "ID": "myid",
                    "Prefix": "",
                    "Status": "Enabled",
                }
            ]
        },
    )
    response = conn.get_bucket_lifecycle_configuration(Bucket="foobar")
    transition = response["Rules"][0]["Transitions"][0]
    transition["Days"].should.equal(30)
    transition["StorageClass"].should.equal("GLACIER")


@mock_s3
def test_lifecycle_multi():
    conn = boto3.client("s3", region_name="us-west-1")
    bucket = conn.create_bucket(
        Bucket="foobar", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    date = "2022-10-12T00:00:00.000Z"
    sc = "GLACIER"

    conn.put_bucket_lifecycle_configuration(
        Bucket="foobar",
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "1",
                    "Prefix": "1/",
                    "Status": "Enabled",
                    "Expiration": {"Days": 1,},
                },
                {
                    "ID": "2",
                    "Prefix": "2/",
                    "Status": "Enabled",
                    "Expiration": {"Days": 2,},
                },
                {
                    "ID": "3",
                    "Prefix": "3/",
                    "Status": "Enabled",
                    "Expiration": {"Date": date,},
                },
                {
                    "ID": "4",
                    "Prefix": "4/",
                    "Status": "Enabled",
                    "Transitions": [{"Days": 4, "StorageClass": sc,},],
                },
                {
                    "ID": "5",
                    "Prefix": "5/",
                    "Status": "Enabled",
                    "Transitions": [{"Date": date, "StorageClass": sc,},],
                },
            ]
        },
    )

    # read the lifecycle back
    rules = conn.get_bucket_lifecycle_configuration(Bucket="foobar")["Rules"]

    for rule in rules:
        if rule["ID"] == "1":
            rule["Prefix"].should.equal("1/")
            rule["Expiration"]["Days"].should.equal(1)
        elif rule["ID"] == "2":
            rule["Prefix"].should.equal("2/")
            rule["Expiration"]["Days"].should.equal(2)
        elif rule["ID"] == "3":
            rule["Prefix"].should.equal("3/")
            rule["Expiration"]["Date"].should.equal(
                datetime(2022, 10, 12, tzinfo=tzutc())
            )
        elif rule["ID"] == "4":
            rule["Prefix"].should.equal("4/")
            rule["Transitions"][0]["Days"].should.equal(4)
            rule["Transitions"][0]["StorageClass"].should.equal(sc)
        elif rule["ID"] == "5":
            rule["Prefix"].should.equal("5/")
            rule["Transitions"][0]["Date"].should.equal(
                datetime(2022, 10, 12, tzinfo=tzutc())
            )
            rule["Transitions"][0]["StorageClass"].should.equal(sc)
        else:
            assert False, "Invalid rule id"


@mock_s3
def test_lifecycle_delete():
    conn = boto3.client("s3", region_name="us-west-1")
    bucket = conn.create_bucket(
        Bucket="foobar", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
    )

    conn.put_bucket_lifecycle_configuration(
        Bucket="foobar",
        LifecycleConfiguration={
            "Rules": [
                {
                    "Expiration": {"Days": 30,},
                    "Status": "Enabled",
                    "Prefix": "",
                    "ID": "test",
                }
            ],
        },
    )
    rules = conn.get_bucket_lifecycle_configuration(Bucket="foobar")["Rules"]
    rules.should.have.length_of(1)

    conn.delete_bucket_lifecycle(Bucket="foobar")
    conn.get_bucket_lifecycle_configuration.when.called_with(
        Bucket="foobar"
    ).should.throw(ClientError)
