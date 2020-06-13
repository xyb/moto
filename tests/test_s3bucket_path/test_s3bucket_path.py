from __future__ import unicode_literals
from six.moves.urllib.request import urlopen
from six.moves.urllib.error import HTTPError

import datetime
from dateutil.tz import tzutc

import boto
import boto3
from botocore.exceptions import ClientError
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

from freezegun import freeze_time
import requests

import sure  # noqa

from moto import mock_s3


class MyModel(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def save(self):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.put_object(Bucket='mybucket', Key=self.name, Body=self.value)


@mock_s3
def test_my_model_save():
    # Create Bucket so that test can run
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket")
    ####################################

    model_instance = MyModel("steve", "is awesome")
    model_instance.save()

    conn.get_object(Bucket="mybucket", Key="steve")["Body"].read().should.equal(
        b"is awesome"
    )


@mock_s3
def test_missing_key():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.get_object.when.called_with(Bucket="foobar", Key="the-key").should.throw(ClientError)


@mock_s3
def test_missing_key_urllib2():
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="foobar")

    urlopen.when.called_with("http://s3.amazonaws.com/foobar/the-key").should.throw(
        HTTPError
    )


@mock_s3
def test_empty_key():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.put_object(Bucket="foobar", Key="the-key", Body="")

    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"")


@mock_s3
def test_empty_key_set_on_existing_key():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.put_object(Bucket="foobar", Key="the-key", Body="foobar")

    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"foobar")

    conn.put_object(Bucket="foobar", Key="the-key", Body="")
    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"")


@mock_s3
def test_large_key_save():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.put_object(Bucket="foobar", Key="the-key", Body="foobar" * 100000)

    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"foobar" * 100000)


@mock_s3
def test_copy_key():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.put_object(Bucket="foobar", Key="the-key", Body="some value")

    conn.copy_object(Bucket="foobar", Key="new-key", CopySource={'Bucket': 'foobar', 'Key': "the-key"})

    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"some value")
    conn.get_object(Bucket="foobar", Key="new-key")["Body"].read().should.equal(b"some value")


@mock_s3
def test_set_metadata():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    conn.put_object(Bucket="foobar", Key="the-key", Body="Testval", Metadata={"md": "Metadatastring"})

    conn.get_object(Bucket="foobar", Key="the-key")["Metadata"]["md"].should.equal("Metadatastring")


@freeze_time("2012-01-01 12:00:00")
@mock_s3
def test_last_modified():
    # See https://github.com/boto/boto/issues/466
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")
    conn.put_object(Bucket="foobar", Key="the-key", Body="some value")

    rs = conn.list_objects(Bucket="foobar")["Contents"]
    rs[0]["LastModified"].should.equal(datetime.datetime(2012, 1, 1, 12, tzinfo=tzutc()))

    conn.get_object(Bucket="foobar", Key="the-key")["LastModified"].should.equal(
        datetime.datetime(2012, 1, 1, 12, tzinfo=tzutc())
    )


@mock_s3
def test_missing_bucket():
    conn = boto3.client("s3", region_name="us-east-1")
    conn.head_bucket.when.called_with(Bucket="mybucket").should.throw(ClientError)


@mock_s3
def test_bucket_with_dash():
    conn = boto3.client("s3", region_name="us-east-1")
    conn.head_bucket.when.called_with(Bucket="mybucket-test").should.throw(ClientError)


@mock_s3
def test_bucket_deletion():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    conn.put_object(Bucket="foobar", Key="the-key", Body="some value")

    # Try to delete a bucket that still has keys
    conn.delete_bucket.when.called_with(Bucket="foobar").should.throw(ClientError)

    conn.delete_object(Bucket="foobar", Key="the-key")
    conn.delete_bucket(Bucket="foobar")

    # Get non-existing bucket
    conn.head_bucket.when.called_with(Bucket="foobar").should.throw(ClientError)

    # Delete non-existent bucket
    conn.delete_bucket.when.called_with(Bucket="foobar").should.throw(ClientError)


@mock_s3
def test_get_all_buckets():
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="foobar")
    conn.create_bucket(Bucket="foobar2")
    buckets = conn.list_buckets()["Buckets"]

    buckets.should.have.length_of(2)


@mock_s3
def test_post_to_bucket():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    requests.post(
        "https://s3.amazonaws.com/foobar", {"key": "the-key", "file": "nothing"}
    )

    conn.get_object(Bucket="foobar", Key="the-key")["Body"].read().should.equal(b"nothing")


@mock_s3
def test_post_with_metadata_to_bucket():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    requests.post(
        "https://s3.amazonaws.com/foobar",
        {"key": "the-key", "file": "nothing", "x-amz-meta-test": "metadata"},
    )

    conn.get_object(Bucket="foobar", Key="the-key")["Metadata"]["test"].should.equal("metadata")


@mock_s3
def test_bucket_name_with_dot():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="firstname.lastname")

    conn.put_object(Bucket="firstname.lastname", Key="somekey", Body="somedata")


@mock_s3
def test_key_with_special_characters():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="test_bucket_name")

    conn.put_object(Bucket="test_bucket_name", Key="test_list_keys_2/*x+?^@~!y", Body="value1")

    key_list = conn.list_objects(
        Bucket="test_bucket_name",
        Prefix="test_list_keys_2/",
        Delimiter="/",
    )["Contents"]
    keys = [x for x in key_list]
    keys[0]["Key"].should.equal("test_list_keys_2/*x+?^@~!y")


@mock_s3
def test_bucket_key_listing_order():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="test_bucket")
    prefix = "toplevel/"

    def store(name):
        conn.put_object(Bucket="test_bucket", Key=prefix + name, Body="somedata")

    names = ["x/key", "y.key1", "y.key2", "y.key3", "x/y/key", "x/y/z/key"]

    for name in names:
        store(name)

    delimiter = None
    keys = [x["Key"] for x in
        conn.list_objects(Bucket="test_bucket", Prefix=prefix)["Contents"]
    ]
    keys.should.equal(
        [
            "toplevel/x/key",
            "toplevel/x/y/key",
            "toplevel/x/y/z/key",
            "toplevel/y.key1",
            "toplevel/y.key2",
            "toplevel/y.key3",
        ]
    )

    def get_keys_and_prefixes(prefix, delimiter):
        args = {}
        if prefix:
            args["Prefix"] = prefix
        if delimiter:
            args["Delimiter"] = delimiter
        objects = conn.list_objects(Bucket="test_bucket", **args)
        prefixes = [prefix["Prefix"] for prefix in objects.get("CommonPrefixes", [])]
        keys = [x["Key"] for x in
            objects.get("Contents", [])
        ]
        return (keys + prefixes)

    delimiter = "/"
    keys = get_keys_and_prefixes(prefix, delimiter)
    keys.should.equal(
        ["toplevel/y.key1", "toplevel/y.key2", "toplevel/y.key3", "toplevel/x/"]
    )

    # Test delimiter with no prefix
    delimiter = "/"
    keys = get_keys_and_prefixes(None, delimiter)
    keys.should.equal(["toplevel/"])

    delimiter = None
    keys = get_keys_and_prefixes(prefix + "x", delimiter)
    keys.should.equal(["toplevel/x/key", "toplevel/x/y/key", "toplevel/x/y/z/key"])

    delimiter = "/"
    keys = get_keys_and_prefixes(prefix + "x", delimiter)
    keys.should.equal(["toplevel/x/"])


@mock_s3
def test_delete_keys():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    conn.put_object(Bucket="foobar", Key="file1", Body="abc")
    conn.put_object(Bucket="foobar", Key="file2", Body="abc")
    conn.put_object(Bucket="foobar", Key="file3", Body="abc")
    conn.put_object(Bucket="foobar", Key="file4", Body="abc")

    result = conn.delete_objects(
        Bucket="foobar",
        Delete={
            "Objects": [{
                "Key": "file2",
            }, {
                "Key": "file3",
            }]
        }
    )
    result["Deleted"].should.have.length_of(2)
    result.get("Errors", []).should.have.length_of(0)
    keys = conn.list_objects(Bucket="foobar")["Contents"]
    keys.should.have.length_of(2)
    keys[0]["Key"].should.equal("file1")


@mock_s3
def test_delete_keys_with_invalid():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket = conn.create_bucket(Bucket="foobar")

    conn.put_object(Bucket="foobar", Key="file1", Body="abc")
    conn.put_object(Bucket="foobar", Key="file2", Body="abc")
    conn.put_object(Bucket="foobar", Key="file3", Body="abc")
    conn.put_object(Bucket="foobar", Key="file4", Body="abc")

    result = conn.delete_objects(
        Bucket="foobar",
        Delete={
            "Objects": [{
                "Key": "abc",
            }, {
                "Key": "file3",
            }]
        }
    )

    result["Deleted"].should.have.length_of(1)
    result["Errors"].should.have.length_of(1)
    keys = conn.list_objects(Bucket="foobar")["Contents"]
    keys.should.have.length_of(3)
    keys[0]["Key"].should.equal("file1")
