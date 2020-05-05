import boto
import boto3
from boto.ec2.cloudwatch.alarm import MetricAlarm
from boto.s3.key import Key
from datetime import datetime
import sure  # noqa

from moto import mock_cloudwatch, mock_s3_deprecated


def alarm_fixture(name="tester", action=None):
    action = action or ["arn:alarm"]
    return dict(
        AlarmName=name,
        Namespace="{0}_namespace".format(name),
        MetricName="{0}_metric".format(name),
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        Threshold=2.0,
        Period=60,
        EvaluationPeriods=5,
        Statistic="Average",
        AlarmDescription="A test",
        Dimensions=[{"Name": "InstanceId", "Value": "i-0123456,i-0123457"}],
        AlarmActions=action,
        OKActions=["arn:ok"],
        InsufficientDataActions=["arn:insufficient"],
        Unit="Seconds",
    )


@mock_cloudwatch
def test_create_alarm():
    conn = boto3.client("cloudwatch", region_name="us-west-1")

    alarm = alarm_fixture()
    conn.put_metric_alarm(**alarm)

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(1)
    alarm = alarms[0]
    alarm['AlarmName'].should.equal("tester")
    alarm['Namespace'].should.equal("tester_namespace")
    alarm['MetricName'].should.equal("tester_metric")
    alarm['ComparisonOperator'].should.equal("GreaterThanOrEqualToThreshold")
    alarm['Threshold'].should.equal(2.0)
    alarm['Period'].should.equal(60)
    alarm['EvaluationPeriods'].should.equal(5)
    alarm['Statistic'].should.equal("Average")
    alarm['AlarmDescription'].should.equal("A test")
    alarm['Dimensions'][0]['Name'].should.equal("InstanceId")
    alarm['Dimensions'][0]['Value'].should.equal("i-0123456,i-0123457")
    list(alarm['AlarmActions']).should.equal(["arn:alarm"])
    list(alarm['OKActions']).should.equal(["arn:ok"])
    list(alarm['InsufficientDataActions']).should.equal(["arn:insufficient"])
    alarm['Unit'].should.equal("Seconds")


@mock_cloudwatch
def test_delete_alarm():
    conn = boto3.client("cloudwatch", region_name="us-west-1")
    cloudwatch = boto3.resource("cloudwatch", region_name="us-west-1")

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(0)

    alarm = alarm_fixture()
    conn.put_metric_alarm(**alarm)

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(1)

    alarm = cloudwatch.Alarm(alarm['AlarmName'])
    alarm.delete()

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(0)


@mock_cloudwatch
def test_put_metric_data():
    conn = boto3.client("cloudwatch", region_name="us-west-1")

    conn.put_metric_data(
        Namespace="tester",
        MetricData=[{
            "MetricName": "metric",
            "Value": 1.5,
            "Dimensions": [{
                "Name": "InstanceId",
                "Value": "i-0123456,i-0123457",
            }],
        }],
    )

    metrics = conn.list_metrics()['Metrics']
    metric_names = [m for m in metrics if m['MetricName'] == "metric"]
    metric_names.should.have(1)
    metric = metrics[0]
    metric['Namespace'].should.equal("tester")
    metric['MetricName'].should.equal("metric")
    dimension = metric['Dimensions'][0]
    dimension['Name'].should.equal("InstanceId")
    dimension['Value'].should.equal("i-0123456,i-0123457")


@mock_cloudwatch
def test_describe_alarms():
    conn = boto3.client("cloudwatch", region_name="us-west-1")

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(0)

    conn.put_metric_alarm(**alarm_fixture(name="nfoobar", action=["afoobar"]))
    conn.put_metric_alarm(**alarm_fixture(name="nfoobaz", action=["afoobaz"]))
    conn.put_metric_alarm(**alarm_fixture(name="nbarfoo", action=["abarfoo"]))
    conn.put_metric_alarm(**alarm_fixture(name="nbazfoo", action=["abazfoo"]))

    enabled = alarm_fixture(name="enabled1", action=["abarfoo"])
    enabled['AlarmActions'].append("arn:alarm")
    enabled['ActionsEnabled'] = True
    conn.put_metric_alarm(**enabled)

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(5)
    alarms = conn.describe_alarms(AlarmNamePrefix="nfoo")['MetricAlarms']
    alarms.should.have.length_of(2)
    alarms = conn.describe_alarms(AlarmNames=["nfoobar", "nbarfoo", "nbazfoo"])['MetricAlarms']
    alarms.should.have.length_of(3)
    alarms = conn.describe_alarms(ActionPrefix="afoo")['MetricAlarms']
    alarms.should.have.length_of(2)
    alarms = conn.describe_alarms(AlarmNamePrefix="enabled")['MetricAlarms']
    alarms.should.have.length_of(1)
    alarms[0]['ActionsEnabled'].should.equal(True)

    for alarm in conn.describe_alarms()['MetricAlarms']:
        conn.delete_alarms(AlarmNames=[alarm['AlarmName']])

    alarms = conn.describe_alarms()['MetricAlarms']
    alarms.should.have.length_of(0)


# TODO: THIS IS CURRENTLY BROKEN!
# @mock_s3_deprecated
# @mock_cloudwatch_deprecated
# def test_cloudwatch_return_s3_metrics():
#
#     region = "us-east-1"
#
#     cw = boto.ec2.cloudwatch.connect_to_region(region)
#     s3 = boto.s3.connect_to_region(region)
#
#     bucket_name_1 = "test-bucket-1"
#     bucket_name_2 = "test-bucket-2"
#
#     bucket1 = s3.create_bucket(bucket_name=bucket_name_1)
#     key = Key(bucket1)
#     key.key = "the-key"
#     key.set_contents_from_string("foobar" * 4)
#     s3.create_bucket(bucket_name=bucket_name_2)
#
#     metrics_s3_bucket_1 = cw.list_metrics(dimensions={"BucketName": bucket_name_1})
#     # Verify that the OOTB S3 metrics are available for the created buckets
#     len(metrics_s3_bucket_1).should.be(2)
#     metric_names = [m.name for m in metrics_s3_bucket_1]
#     sorted(metric_names).should.equal(
#         ["Metric:BucketSizeBytes", "Metric:NumberOfObjects"]
#     )
#
#     # Explicit clean up - the metrics for these buckets are messing with subsequent tests
#     key.delete()
#     s3.delete_bucket(bucket_name_1)
#     s3.delete_bucket(bucket_name_2)
