from __future__ import unicode_literals
import boto3
import botocore
import boto
import boto.ec2.elb
from boto.ec2.elb import HealthCheck
from boto.ec2.elb.attributes import (
    ConnectionSettingAttribute,
    ConnectionDrainingAttribute,
    AccessLogAttribute,
)
from botocore.exceptions import ClientError
from boto.exception import BotoServerError
from nose.tools import assert_raises
import sure  # noqa

from moto import mock_elb, mock_ec2
from moto.core import ACCOUNT_ID


@mock_elb
@mock_ec2
def test_create_load_balancer():
    conn = boto3.client("elb", region_name="us-east-1")
    ec2 = boto3.client("ec2", region_name="us-east-1")

    security_group_id = ec2.create_security_group(
        GroupName="sg-abc987", Description="description"
    )["GroupId"]

    zones = ["us-east-1a", "us-east-1b"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb",
        AvailabilityZones=zones,
        Listeners=ports,
        Scheme="internal",
        SecurityGroups=[security_group_id],
    )

    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancer = balancers[0]
    balancer["LoadBalancerName"].should.equal("my-lb")
    balancer["Scheme"].should.equal("internal")
    list(balancer["SecurityGroups"]).should.equal([security_group_id])
    set(balancer["AvailabilityZones"]).should.equal(set(["us-east-1a", "us-east-1b"]))
    listener1 = balancer["ListenerDescriptions"][0]
    listener1["Listener"]["LoadBalancerPort"].should.equal(80)
    listener1["Listener"]["InstancePort"].should.equal(8080)
    listener1["Listener"]["Protocol"].should.equal("HTTP")
    listener2 = balancer["ListenerDescriptions"][1]
    listener2["Listener"]["LoadBalancerPort"].should.equal(443)
    listener2["Listener"]["InstancePort"].should.equal(8443)
    listener2["Listener"]["Protocol"].should.equal("TCP")


@mock_elb
def test_getting_missing_elb():
    conn = boto3.client("elb", region_name="us-east-1")
    conn.describe_load_balancers.when.called_with(
        LoadBalancerNames=["aaa"]
    ).should.throw(ClientError)


@mock_elb
def test_create_elb_in_multiple_region():
    zones = ["us-east-1a", "us-east-1b"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]

    west1_conn = boto3.client("elb", region_name="us-west-1")
    west1_conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )

    west2_conn = boto3.client("elb", region_name="us-west-2")
    west2_conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )

    list(
        west1_conn.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(1)
    list(
        west2_conn.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(1)


@mock_elb
def test_create_load_balancer_with_certificate():
    conn = boto3.client("elb", region_name="us-east-1")

    zones = ["us-east-1a"]
    ports = [
        {
            "LoadBalancerPort": 443,
            "InstancePort": 8443,
            "Protocol": "https",
            "SSLCertificateId": "arn:aws:iam:{}:server-certificate/test-cert".format(
                ACCOUNT_ID
            ),
        }
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )

    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancer = balancers[0]
    balancer["LoadBalancerName"].should.equal("my-lb")
    balancer["Scheme"].should.equal("internet-facing")
    set(balancer["AvailabilityZones"]).should.equal(set(["us-east-1a"]))
    listener = balancer["ListenerDescriptions"][0]["Listener"]
    listener["LoadBalancerPort"].should.equal(443)
    listener["InstancePort"].should.equal(8443)
    listener["Protocol"].should.equal("HTTPS")
    listener["SSLCertificateId"].should.equal(
        "arn:aws:iam:{}:server-certificate/test-cert".format(ACCOUNT_ID)
    )


@mock_elb
def test_create_and_delete_boto3_support():
    client = boto3.client("elb", region_name="us-east-1")

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )
    list(
        client.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(1)

    client.delete_load_balancer(LoadBalancerName="my-lb")
    list(
        client.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(0)


@mock_elb
def test_create_load_balancer_with_no_listeners_defined():
    client = boto3.client("elb", region_name="us-east-1")

    with assert_raises(ClientError):
        client.create_load_balancer(
            LoadBalancerName="my-lb",
            Listeners=[],
            AvailabilityZones=["us-east-1a", "us-east-1b"],
        )


@mock_elb
def test_describe_paginated_balancers():
    client = boto3.client("elb", region_name="us-east-1")

    for i in range(51):
        client.create_load_balancer(
            LoadBalancerName="my-lb%d" % i,
            Listeners=[
                {"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}
            ],
            AvailabilityZones=["us-east-1a", "us-east-1b"],
        )

    resp = client.describe_load_balancers()
    resp["LoadBalancerDescriptions"].should.have.length_of(50)
    resp["NextMarker"].should.equal(
        resp["LoadBalancerDescriptions"][-1]["LoadBalancerName"]
    )
    resp2 = client.describe_load_balancers(Marker=resp["NextMarker"])
    resp2["LoadBalancerDescriptions"].should.have.length_of(1)
    assert "NextToken" not in resp2.keys()


@mock_elb
@mock_ec2
def test_apply_security_groups_to_load_balancer():
    client = boto3.client("elb", region_name="us-east-1")
    ec2 = boto3.resource("ec2", region_name="us-east-1")

    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
    security_group = ec2.create_security_group(
        GroupName="sg01", Description="Test security group sg01", VpcId=vpc.id
    )

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )

    response = client.apply_security_groups_to_load_balancer(
        LoadBalancerName="my-lb", SecurityGroups=[security_group.id]
    )

    assert response["SecurityGroups"] == [security_group.id]
    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    assert balancer["SecurityGroups"] == [security_group.id]

    # Using a not-real security group raises an error
    with assert_raises(ClientError) as error:
        response = client.apply_security_groups_to_load_balancer(
            LoadBalancerName="my-lb", SecurityGroups=["not-really-a-security-group"]
        )
    assert "One or more of the specified security groups do not exist." in str(
        error.exception
    )


@mock_elb
def test_add_listener():
    conn = boto3.client("elb", region_name="us-east-1")
    zones = ["us-east-1a", "us-east-1b"]
    ports = [{"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080}]

    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )
    new_listener = [{"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443}]
    conn.create_load_balancer_listeners(
        LoadBalancerName="my-lb", Listeners=new_listener
    )
    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancer = balancers[0]
    listener1 = balancer["ListenerDescriptions"][0]
    listener1["Listener"]["LoadBalancerPort"].should.equal(80)
    listener1["Listener"]["InstancePort"].should.equal(8080)
    listener1["Listener"]["Protocol"].should.equal("HTTP")
    listener2 = balancer["ListenerDescriptions"][1]
    listener2["Listener"]["LoadBalancerPort"].should.equal(443)
    listener2["Listener"]["InstancePort"].should.equal(8443)
    listener2["Listener"]["Protocol"].should.equal("TCP")


@mock_elb
def test_delete_listener():
    conn = boto3.client("elb", region_name="us-east-1")

    zones = ["us-east-1a", "us-east-1b"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )
    conn.delete_load_balancer_listeners(
        LoadBalancerName="my-lb", LoadBalancerPorts=[443]
    )
    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancer = balancers[0]
    listener1 = balancer["ListenerDescriptions"][0]
    listener1["Listener"]["LoadBalancerPort"].should.equal(80)
    listener1["Listener"]["InstancePort"].should.equal(8080)
    listener1["Listener"]["Protocol"].should.equal("HTTP")
    balancer["ListenerDescriptions"].should.have.length_of(1)


@mock_elb
def test_create_and_delete_listener_boto3_support():
    client = boto3.client("elb", region_name="us-east-1")

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )
    list(
        client.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(1)

    client.create_load_balancer_listeners(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443}],
    )
    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    list(balancer["ListenerDescriptions"]).should.have.length_of(2)
    balancer["ListenerDescriptions"][0]["Listener"]["Protocol"].should.equal("HTTP")
    balancer["ListenerDescriptions"][0]["Listener"]["LoadBalancerPort"].should.equal(80)
    balancer["ListenerDescriptions"][0]["Listener"]["InstancePort"].should.equal(8080)
    balancer["ListenerDescriptions"][1]["Listener"]["Protocol"].should.equal("TCP")
    balancer["ListenerDescriptions"][1]["Listener"]["LoadBalancerPort"].should.equal(
        443
    )
    balancer["ListenerDescriptions"][1]["Listener"]["InstancePort"].should.equal(8443)

    # Creating this listener with an conflicting definition throws error
    with assert_raises(ClientError):
        client.create_load_balancer_listeners(
            LoadBalancerName="my-lb",
            Listeners=[
                {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 1234}
            ],
        )

    client.delete_load_balancer_listeners(
        LoadBalancerName="my-lb", LoadBalancerPorts=[443]
    )

    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    list(balancer["ListenerDescriptions"]).should.have.length_of(1)


@mock_elb
def test_set_sslcertificate():
    conn = boto3.client("elb", region_name="us-east-1")

    zones = ["us-east-1a", "us-east-1b"]
    ports = [{"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443}]
    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )
    conn.set_load_balancer_listener_ssl_certificate(
        LoadBalancerName="my-lb",
        LoadBalancerPort=443,
        SSLCertificateId="arn:certificate",
    )
    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancer = balancers[0]
    listener1 = balancer["ListenerDescriptions"][0]
    listener1["Listener"]["LoadBalancerPort"].should.equal(443)
    listener1["Listener"]["InstancePort"].should.equal(8443)
    listener1["Listener"]["Protocol"].should.equal("TCP")
    listener1["Listener"]["SSLCertificateId"].should.equal("arn:certificate")


@mock_elb
def test_get_load_balancers_by_name():
    conn = boto3.client("elb", region_name="us-east-1")

    zones = ["us-east-1a", "us-east-1b"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb1", AvailabilityZones=zones, Listeners=ports
    )
    conn.create_load_balancer(
        LoadBalancerName="my-lb2", AvailabilityZones=zones, Listeners=ports
    )
    conn.create_load_balancer(
        LoadBalancerName="my-lb3", AvailabilityZones=zones, Listeners=ports
    )

    conn.describe_load_balancers()["LoadBalancerDescriptions"].should.have.length_of(3)
    conn.describe_load_balancers(LoadBalancerNames=["my-lb1"])[
        "LoadBalancerDescriptions"
    ].should.have.length_of(1)
    conn.describe_load_balancers(LoadBalancerNames=["my-lb1", "my-lb2"])[
        "LoadBalancerDescriptions"
    ].should.have.length_of(2)


@mock_elb
def test_delete_load_balancer():
    conn = boto3.client("elb", region_name="us-east-1")

    zones = ["us-east-1a"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )

    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancers.should.have.length_of(1)

    conn.delete_load_balancer(LoadBalancerName="my-lb")
    balancers = conn.describe_load_balancers()["LoadBalancerDescriptions"]
    balancers.should.have.length_of(0)


@mock_elb
def test_create_health_check():
    conn = boto3.client("elb", region_name="us-east-1")
    hc = {
        "Interval": 20,
        "HealthyThreshold": 3,
        "UnhealthyThreshold": 5,
        "Target": "HTTP:8080/health",
        "Timeout": 23,
    }

    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    conn.configure_health_check(LoadBalancerName="my-lb", HealthCheck=hc)

    balancer = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    health_check = balancer["HealthCheck"]
    health_check["Interval"].should.equal(20)
    health_check["HealthyThreshold"].should.equal(3)
    health_check["UnhealthyThreshold"].should.equal(5)
    health_check["Target"].should.equal("HTTP:8080/health")
    health_check["Timeout"].should.equal(23)


@mock_elb
def test_create_health_check_boto3():
    client = boto3.client("elb", region_name="us-east-1")

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )
    client.configure_health_check(
        LoadBalancerName="my-lb",
        HealthCheck={
            "Target": "HTTP:8080/health",
            "Interval": 20,
            "Timeout": 23,
            "HealthyThreshold": 3,
            "UnhealthyThreshold": 5,
        },
    )

    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    balancer["HealthCheck"]["Target"].should.equal("HTTP:8080/health")
    balancer["HealthCheck"]["Interval"].should.equal(20)
    balancer["HealthCheck"]["Timeout"].should.equal(23)
    balancer["HealthCheck"]["HealthyThreshold"].should.equal(3)
    balancer["HealthCheck"]["UnhealthyThreshold"].should.equal(5)


@mock_ec2
@mock_elb
def test_register_instances():
    ec2_conn = boto3.client("ec2", region_name="us-east-1")
    reservation = ec2_conn.run_instances(ImageId="ami-1234abcd", MaxCount=2, MinCount=2)
    instance_id1 = reservation["Instances"][0]["InstanceId"]
    instance_id2 = reservation["Instances"][1]["InstanceId"]

    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )

    conn.register_instances_with_load_balancer(
        LoadBalancerName="my-lb",
        Instances=[{"InstanceId": instance_id1}, {"InstanceId": instance_id2}],
    )

    balancer = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    instance_ids = [instance["InstanceId"] for instance in balancer["Instances"]]
    set(instance_ids).should.equal(set([instance_id1, instance_id2]))


@mock_ec2
@mock_elb
def test_register_instances_boto3():
    ec2 = boto3.resource("ec2", region_name="us-east-1")
    response = ec2.create_instances(ImageId="ami-1234abcd", MinCount=2, MaxCount=2)
    instance_id1 = response[0].id
    instance_id2 = response[1].id

    client = boto3.client("elb", region_name="us-east-1")
    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )
    client.register_instances_with_load_balancer(
        LoadBalancerName="my-lb",
        Instances=[{"InstanceId": instance_id1}, {"InstanceId": instance_id2}],
    )
    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    instance_ids = [instance["InstanceId"] for instance in balancer["Instances"]]
    set(instance_ids).should.equal(set([instance_id1, instance_id2]))


@mock_ec2
@mock_elb
def test_deregister_instances():
    ec2_conn = boto3.client("ec2", region_name="us-east-1")
    reservation = ec2_conn.run_instances(ImageId="ami-1234abcd", MinCount=2, MaxCount=2)
    instance_id1 = reservation["Instances"][0]["InstanceId"]
    instance_id2 = reservation["Instances"][1]["InstanceId"]

    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )

    conn.register_instances_with_load_balancer(
        LoadBalancerName="my-lb",
        Instances=[{"InstanceId": instance_id1}, {"InstanceId": instance_id2}],
    )

    balancer = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    balancer["Instances"].should.have.length_of(2)
    conn.deregister_instances_from_load_balancer(
        LoadBalancerName="my-lb", Instances=[{"InstanceId": instance_id1}],
    )

    balancer = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    balancer["Instances"].should.have.length_of(1)
    balancer["Instances"][0]["InstanceId"].should.equal(instance_id2)


@mock_ec2
@mock_elb
def test_deregister_instances_boto3():
    ec2 = boto3.resource("ec2", region_name="us-east-1")
    response = ec2.create_instances(ImageId="ami-1234abcd", MinCount=2, MaxCount=2)
    instance_id1 = response[0].id
    instance_id2 = response[1].id

    client = boto3.client("elb", region_name="us-east-1")
    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )
    client.register_instances_with_load_balancer(
        LoadBalancerName="my-lb",
        Instances=[{"InstanceId": instance_id1}, {"InstanceId": instance_id2}],
    )

    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    balancer["Instances"].should.have.length_of(2)

    client.deregister_instances_from_load_balancer(
        LoadBalancerName="my-lb", Instances=[{"InstanceId": instance_id1}]
    )

    balancer = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    balancer["Instances"].should.have.length_of(1)
    balancer["Instances"][0]["InstanceId"].should.equal(instance_id2)


@mock_elb
def test_default_attributes():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]

    attributes["CrossZoneLoadBalancing"]["Enabled"].should.be.false
    attributes["ConnectionDraining"]["Enabled"].should.be.false
    attributes["AccessLog"]["Enabled"].should.be.false
    attributes["ConnectionSettings"]["IdleTimeout"].should.equal(60)


@mock_elb
def test_cross_zone_load_balancing_attribute():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"CrossZoneLoadBalancing": {"Enabled": True}},
    )
    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["CrossZoneLoadBalancing"]["Enabled"].should.be.true

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"CrossZoneLoadBalancing": {"Enabled": False}},
    )
    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["CrossZoneLoadBalancing"]["Enabled"].should.be.false


@mock_elb
def test_connection_draining_attribute():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionDraining": {"Enabled": True, "Timeout": 60}},
    )

    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["ConnectionDraining"]["Enabled"].should.be.true
    attributes["ConnectionDraining"]["Timeout"].should.equal(60)

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionDraining": {"Enabled": True, "Timeout": 30}},
    )

    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["ConnectionDraining"]["Timeout"].should.equal(30)

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={
            "ConnectionDraining": {"Enabled": False, "Timeout": 30}
        },
    )
    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["ConnectionDraining"]["Enabled"].should.be.false


@mock_elb
def test_access_log_attribute():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={
            "AccessLog": {
                "Enabled": True,
                "S3BucketName": "bucket",
                "S3BucketPrefix": "prefix",
                "EmitInterval": 123,
            }
        },
    )

    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={
            "AccessLog": {
                "Enabled": True,
                "S3BucketName": "bucket",
                "S3BucketPrefix": "prefix",
                "EmitInterval": 60,
            }
        },
    )

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={
            "AccessLog": {
                "Enabled": False,
                "S3BucketName": "bucket",
                "S3BucketPrefix": "prefix",
                "EmitInterval": 60,
            }
        },
    )
    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["AccessLog"]["Enabled"].should.be.false


@mock_elb
def test_connection_settings_attribute():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionSettings": {"IdleTimeout": 120}},
    )

    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["ConnectionSettings"]["IdleTimeout"].should.equal(120)

    conn.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionSettings": {"IdleTimeout": 60}},
    )

    attributes = conn.describe_load_balancer_attributes(LoadBalancerName="my-lb",)[
        "LoadBalancerAttributes"
    ]
    attributes["ConnectionSettings"]["IdleTimeout"].should.equal(60)


@mock_elb
def test_create_lb_cookie_stickiness_policy():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    cookie_expiration_period = 60
    policy_name = "LBCookieStickinessPolicy"

    conn.create_lb_cookie_stickiness_policy(
        LoadBalancerName="my-lb",
        CookieExpirationPeriod=cookie_expiration_period,
        PolicyName=policy_name,
    )
    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    # There appears to be a quirk about boto, whereby it returns a unicode
    # string for cookie_expiration_period, despite being stated in
    # documentation to be a long numeric.
    #
    # To work around that, this value is converted to an int and checked.
    cookie_expiration_period_response_str = lb["Policies"][
        "LBCookieStickinessPolicies"
    ][0]["CookieExpirationPeriod"]
    int(cookie_expiration_period_response_str).should.equal(cookie_expiration_period)
    lb["Policies"]["LBCookieStickinessPolicies"][0]["PolicyName"].should.equal(
        policy_name
    )


@mock_elb
def test_create_lb_cookie_stickiness_policy_no_expiry():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    policy_name = "LBCookieStickinessPolicy"
    conn.create_lb_cookie_stickiness_policy(
        LoadBalancerName="my-lb", CookieExpirationPeriod=0, PolicyName=policy_name,
    )

    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    lb["Policies"]["LBCookieStickinessPolicies"][0].get(
        "CookieExpirationPeriod"
    ).should.be.none
    lb["Policies"]["LBCookieStickinessPolicies"][0]["PolicyName"].should.equal(
        policy_name
    )


@mock_elb
def test_create_app_cookie_stickiness_policy():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    cookie_name = "my-stickiness-policy"
    policy_name = "AppCookieStickinessPolicy"

    conn.create_app_cookie_stickiness_policy(
        LoadBalancerName="my-lb", CookieName=cookie_name, PolicyName=policy_name,
    )

    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    lb["Policies"]["AppCookieStickinessPolicies"][0]["CookieName"].should.equal(
        cookie_name
    )
    lb["Policies"]["AppCookieStickinessPolicies"][0]["PolicyName"].should.equal(
        policy_name
    )


@mock_elb
def test_create_lb_policy():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    policy_name = "ProxyPolicy"

    conn.create_load_balancer_policy(
        LoadBalancerName="my-lb",
        PolicyName=policy_name,
        PolicyTypeName="ProxyProtocolPolicyType",
        PolicyAttributes=[
            {"AttributeName": "ProxyProtocol", "AttributeValue": "True",}
        ],
    )

    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    lb["Policies"]["OtherPolicies"][0].should.equal(policy_name)


@mock_elb
def test_set_policies_of_listener():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    listener_port = 80
    policy_name = "my-stickiness-policy"

    # boto docs currently state that zero or one policy may be associated
    # with a given listener

    # in a real flow, it is necessary first to create a policy,
    # then to set that policy to the listener
    conn.create_lb_cookie_stickiness_policy(
        LoadBalancerName="my-lb", CookieExpirationPeriod=0, PolicyName=policy_name,
    )
    conn.set_load_balancer_policies_of_listener(
        LoadBalancerName="my-lb",
        LoadBalancerPort=listener_port,
        PolicyNames=[policy_name],
    )

    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    listener = lb["ListenerDescriptions"][0]["Listener"]
    listener["LoadBalancerPort"].should.equal(listener_port)
    # by contrast to a backend, a listener stores only policy name strings
    lb["Policies"]["LBCookieStickinessPolicies"][0]["PolicyName"].should.equal(
        policy_name
    )


@mock_elb
def test_set_policies_of_backend_server():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    instance_port = 8080
    policy_name = "ProxyPolicy"

    # in a real flow, it is necessary first to create a policy,
    # then to set that policy to the backend
    conn.create_load_balancer_policy(
        LoadBalancerName="my-lb",
        PolicyName=policy_name,
        PolicyTypeName="ProxyProtocolPolicyType",
        PolicyAttributes=[
            {"AttributeName": "ProxyProtocol", "AttributeValue": "True",},
        ],
    )
    conn.set_load_balancer_policies_for_backend_server(
        LoadBalancerName="my-lb", InstancePort=instance_port, PolicyNames=[policy_name]
    )

    lb = conn.describe_load_balancers()["LoadBalancerDescriptions"][0]
    backend = lb["BackendServerDescriptions"][0]
    backend["InstancePort"].should.equal(instance_port)
    # by contrast to a listener, a backend stores OtherPolicy objects
    backend["PolicyNames"][0].should.equal(policy_name)


@mock_ec2
@mock_elb
def test_describe_instance_health():
    ec2_conn = boto3.client("ec2", region_name="us-east-1")
    reservation = ec2_conn.run_instances(ImageId="ami-1234abcd", MinCount=2, MaxCount=2)
    instance_id1 = reservation["Instances"][0]["InstanceId"]
    instance_id2 = reservation["Instances"][1]["InstanceId"]

    conn = boto3.client("elb", region_name="us-east-1")
    zones = ["us-east-1a", "us-east-1b"]
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    lb = conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=zones, Listeners=ports
    )

    instances_health = conn.describe_instance_health(LoadBalancerName="my-lb")[
        "InstanceStates"
    ]
    instances_health.should.be.empty

    conn.register_instances_with_load_balancer(
        LoadBalancerName="my-lb",
        Instances=[{"InstanceId": instance_id1}, {"InstanceId": instance_id2}],
    )

    instances_health = conn.describe_instance_health(LoadBalancerName="my-lb")[
        "InstanceStates"
    ]
    instances_health.should.have.length_of(2)
    for instance_health in instances_health:
        instance_health["InstanceId"].should.be.within([instance_id1, instance_id2])
        instance_health["State"].should.equal("InService")

    instances_health = conn.describe_instance_health(
        LoadBalancerName="my-lb", Instances=[{"InstanceId": instance_id1}],
    )["InstanceStates"]
    instances_health.should.have.length_of(1)
    instances_health[0]["InstanceId"].should.equal(instance_id1)
    instances_health[0]["State"].should.equal("InService")


@mock_ec2
@mock_elb
def test_describe_instance_health_boto3():
    elb = boto3.client("elb", region_name="us-east-1")
    ec2 = boto3.client("ec2", region_name="us-east-1")
    instances = ec2.run_instances(MinCount=2, MaxCount=2)["Instances"]
    lb_name = "my_load_balancer"
    elb.create_load_balancer(
        Listeners=[{"InstancePort": 80, "LoadBalancerPort": 8080, "Protocol": "HTTP"}],
        LoadBalancerName=lb_name,
    )
    elb.register_instances_with_load_balancer(
        LoadBalancerName=lb_name, Instances=[{"InstanceId": instances[0]["InstanceId"]}]
    )
    instances_health = elb.describe_instance_health(
        LoadBalancerName=lb_name,
        Instances=[{"InstanceId": instance["InstanceId"]} for instance in instances],
    )
    instances_health["InstanceStates"].should.have.length_of(2)
    instances_health["InstanceStates"][0]["InstanceId"].should.equal(
        instances[0]["InstanceId"]
    )
    instances_health["InstanceStates"][0]["State"].should.equal("InService")
    instances_health["InstanceStates"][1]["InstanceId"].should.equal(
        instances[1]["InstanceId"]
    )
    instances_health["InstanceStates"][1]["State"].should.equal("Unknown")


@mock_elb
def test_add_remove_tags():
    client = boto3.client("elb", region_name="us-east-1")

    client.add_tags.when.called_with(
        LoadBalancerNames=["my-lb"], Tags=[{"Key": "a", "Value": "b"}]
    ).should.throw(botocore.exceptions.ClientError)

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )

    list(
        client.describe_load_balancers()["LoadBalancerDescriptions"]
    ).should.have.length_of(1)

    client.add_tags(LoadBalancerNames=["my-lb"], Tags=[{"Key": "a", "Value": "b"}])

    tags = dict(
        [
            (d["Key"], d["Value"])
            for d in client.describe_tags(LoadBalancerNames=["my-lb"])[
                "TagDescriptions"
            ][0]["Tags"]
        ]
    )
    tags.should.have.key("a").which.should.equal("b")

    client.add_tags(
        LoadBalancerNames=["my-lb"],
        Tags=[
            {"Key": "a", "Value": "b"},
            {"Key": "b", "Value": "b"},
            {"Key": "c", "Value": "b"},
            {"Key": "d", "Value": "b"},
            {"Key": "e", "Value": "b"},
            {"Key": "f", "Value": "b"},
            {"Key": "g", "Value": "b"},
            {"Key": "h", "Value": "b"},
            {"Key": "i", "Value": "b"},
            {"Key": "j", "Value": "b"},
        ],
    )

    client.add_tags.when.called_with(
        LoadBalancerNames=["my-lb"], Tags=[{"Key": "k", "Value": "b"}]
    ).should.throw(botocore.exceptions.ClientError)

    client.add_tags(LoadBalancerNames=["my-lb"], Tags=[{"Key": "j", "Value": "c"}])

    tags = dict(
        [
            (d["Key"], d["Value"])
            for d in client.describe_tags(LoadBalancerNames=["my-lb"])[
                "TagDescriptions"
            ][0]["Tags"]
        ]
    )

    tags.should.have.key("a").which.should.equal("b")
    tags.should.have.key("b").which.should.equal("b")
    tags.should.have.key("c").which.should.equal("b")
    tags.should.have.key("d").which.should.equal("b")
    tags.should.have.key("e").which.should.equal("b")
    tags.should.have.key("f").which.should.equal("b")
    tags.should.have.key("g").which.should.equal("b")
    tags.should.have.key("h").which.should.equal("b")
    tags.should.have.key("i").which.should.equal("b")
    tags.should.have.key("j").which.should.equal("c")
    tags.shouldnt.have.key("k")

    client.remove_tags(LoadBalancerNames=["my-lb"], Tags=[{"Key": "a"}])

    tags = dict(
        [
            (d["Key"], d["Value"])
            for d in client.describe_tags(LoadBalancerNames=["my-lb"])[
                "TagDescriptions"
            ][0]["Tags"]
        ]
    )

    tags.shouldnt.have.key("a")
    tags.should.have.key("b").which.should.equal("b")
    tags.should.have.key("c").which.should.equal("b")
    tags.should.have.key("d").which.should.equal("b")
    tags.should.have.key("e").which.should.equal("b")
    tags.should.have.key("f").which.should.equal("b")
    tags.should.have.key("g").which.should.equal("b")
    tags.should.have.key("h").which.should.equal("b")
    tags.should.have.key("i").which.should.equal("b")
    tags.should.have.key("j").which.should.equal("c")

    client.create_load_balancer(
        LoadBalancerName="other-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 433, "InstancePort": 8433}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )

    client.add_tags(
        LoadBalancerNames=["other-lb"], Tags=[{"Key": "other", "Value": "something"}]
    )

    lb_tags = dict(
        [
            (l["LoadBalancerName"], dict([(d["Key"], d["Value"]) for d in l["Tags"]]))
            for l in client.describe_tags(LoadBalancerNames=["my-lb", "other-lb"])[
                "TagDescriptions"
            ]
        ]
    )

    lb_tags.should.have.key("my-lb")
    lb_tags.should.have.key("other-lb")

    lb_tags["my-lb"].shouldnt.have.key("other")
    lb_tags["other-lb"].should.have.key("other").which.should.equal("something")


@mock_elb
def test_create_with_tags():
    client = boto3.client("elb", region_name="us-east-1")

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
        Tags=[{"Key": "k", "Value": "v"}],
    )

    tags = dict(
        (d["Key"], d["Value"])
        for d in client.describe_tags(LoadBalancerNames=["my-lb"])["TagDescriptions"][
            0
        ]["Tags"]
    )
    tags.should.have.key("k").which.should.equal("v")


@mock_elb
def test_modify_attributes():
    client = boto3.client("elb", region_name="us-east-1")

    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        AvailabilityZones=["us-east-1a", "us-east-1b"],
    )

    # Default ConnectionDraining timeout of 300 seconds
    client.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionDraining": {"Enabled": True}},
    )
    lb_attrs = client.describe_load_balancer_attributes(LoadBalancerName="my-lb")
    lb_attrs["LoadBalancerAttributes"]["ConnectionDraining"]["Enabled"].should.equal(
        True
    )
    lb_attrs["LoadBalancerAttributes"]["ConnectionDraining"]["Timeout"].should.equal(
        300
    )

    # specify a custom ConnectionDraining timeout
    client.modify_load_balancer_attributes(
        LoadBalancerName="my-lb",
        LoadBalancerAttributes={"ConnectionDraining": {"Enabled": True, "Timeout": 45}},
    )
    lb_attrs = client.describe_load_balancer_attributes(LoadBalancerName="my-lb")
    lb_attrs["LoadBalancerAttributes"]["ConnectionDraining"]["Enabled"].should.equal(
        True
    )
    lb_attrs["LoadBalancerAttributes"]["ConnectionDraining"]["Timeout"].should.equal(45)


@mock_ec2
@mock_elb
def test_subnets():
    ec2 = boto3.resource("ec2", region_name="us-east-1")
    vpc = ec2.create_vpc(CidrBlock="172.28.7.0/24", InstanceTenancy="default")
    subnet = ec2.create_subnet(VpcId=vpc.id, CidrBlock="172.28.7.192/26")
    client = boto3.client("elb", region_name="us-east-1")
    client.create_load_balancer(
        LoadBalancerName="my-lb",
        Listeners=[{"Protocol": "tcp", "LoadBalancerPort": 80, "InstancePort": 8080}],
        Subnets=[subnet.id],
    )

    lb = client.describe_load_balancers()["LoadBalancerDescriptions"][0]
    lb.should.have.key("Subnets").which.should.have.length_of(1)
    lb["Subnets"][0].should.equal(subnet.id)

    lb.should.have.key("VPCId").which.should.equal(vpc.id)


@mock_elb
def test_create_load_balancer_duplicate():
    conn = boto3.client("elb", region_name="us-east-1")
    ports = [
        {"Protocol": "http", "LoadBalancerPort": 80, "InstancePort": 8080},
        {"Protocol": "tcp", "LoadBalancerPort": 443, "InstancePort": 8443},
    ]
    conn.create_load_balancer(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports
    )
    conn.create_load_balancer.when.called_with(
        LoadBalancerName="my-lb", AvailabilityZones=[], Listeners=ports,
    ).should.throw(ClientError)
