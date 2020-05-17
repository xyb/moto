from __future__ import unicode_literals
import boto.ec2
import boto.ec2.autoscale
import boto.ec2.elb
import sure
import boto3
from boto3 import Session

from moto import mock_ec2_deprecated, mock_autoscaling, mock_elb

from moto.ec2 import ec2_backends


def test_use_boto_regions():
    boto_regions = set()
    for region in Session().get_available_regions("ec2"):
        boto_regions.add(region)
    for region in Session().get_available_regions("ec2", partition_name="aws-us-gov"):
        boto_regions.add(region)
    for region in Session().get_available_regions("ec2", partition_name="aws-cn"):
        boto_regions.add(region)
    moto_regions = set(ec2_backends)

    moto_regions.should.equal(boto_regions)


def add_servers_to_region(ami_id, count, region):
    conn = boto.ec2.connect_to_region(region)
    for index in range(count):
        conn.run_instances(ami_id)


@mock_ec2_deprecated
def test_add_servers_to_a_single_region():
    region = "ap-northeast-1"
    add_servers_to_region("ami-1234abcd", 1, region)
    add_servers_to_region("ami-5678efgh", 1, region)

    conn = boto.ec2.connect_to_region(region)
    reservations = conn.get_all_instances()
    len(reservations).should.equal(2)
    reservations.sort(key=lambda x: x.instances[0].image_id)

    reservations[0].instances[0].image_id.should.equal("ami-1234abcd")
    reservations[1].instances[0].image_id.should.equal("ami-5678efgh")


@mock_ec2_deprecated
def test_add_servers_to_multiple_regions():
    region1 = "us-east-1"
    region2 = "ap-northeast-1"
    add_servers_to_region("ami-1234abcd", 1, region1)
    add_servers_to_region("ami-5678efgh", 1, region2)

    us_conn = boto.ec2.connect_to_region(region1)
    ap_conn = boto.ec2.connect_to_region(region2)
    us_reservations = us_conn.get_all_instances()
    ap_reservations = ap_conn.get_all_instances()

    len(us_reservations).should.equal(1)
    len(ap_reservations).should.equal(1)

    us_reservations[0].instances[0].image_id.should.equal("ami-1234abcd")
    ap_reservations[0].instances[0].image_id.should.equal("ami-5678efgh")


@mock_autoscaling
@mock_elb
def test_create_autoscaling_groups_in_different_regions():
    elb_conn = boto3.client("elb", region_name="us-east-1")
    elb_conn.create_load_balancer(
        LoadBalancerName="us_test_lb",
        AvailabilityZones=[],
        Listeners=[{"LoadBalancerPort": 80, "InstancePort": 8080, "Protocol": "http"}],
    )
    elb_conn = boto3.client("elb", region_name="ap-northeast-1")
    elb_conn.create_load_balancer(
        LoadBalancerName="ap_test_lb",
        AvailabilityZones=[],
        Listeners=[{"LoadBalancerPort": 80, "InstancePort": 8080, "Protocol": "http"}],
    )

    us_conn = boto3.client("autoscaling", region_name="us-east-1")
    config = {
        "LaunchConfigurationName": "us_tester",
        "ImageId": "ami-abcd1234",
        "InstanceType": "m1.small",
    }
    x = us_conn.create_launch_configuration(**config)

    us_subnet_id = list(ec2_backends["us-east-1"].subnets["us-east-1c"].keys())[0]
    ap_subnet_id = list(
        ec2_backends["ap-northeast-1"].subnets["ap-northeast-1a"].keys()
    )[0]
    group = {
        "AutoScalingGroupName": "us_tester_group",
        "AvailabilityZones": ["us-east-1c"],
        "MaxSize": 2,
        "MinSize": 2,
        "LaunchConfigurationName": "us_tester",
        "LoadBalancerNames": ["us_test_lb"],
        "PlacementGroup": "us_test_placement",
        "VPCZoneIdentifier": us_subnet_id,
    }
    us_conn.create_auto_scaling_group(**group)

    ap_conn = boto3.client("autoscaling", region_name="ap-northeast-1")
    config = {
        "LaunchConfigurationName": "ap_tester",
        "ImageId": "ami-efgh5678",
        "InstanceType": "m1.small",
    }
    ap_conn.create_launch_configuration(**config)

    group = {
        "AutoScalingGroupName": "ap_tester_group",
        "AvailabilityZones": ["ap-northeast-1a"],
        "MaxSize": 2,
        "MinSize": 2,
        "LaunchConfigurationName": "ap_tester",
        "LoadBalancerNames": ["ap_test_lb"],
        "PlacementGroup": "ap_test_placement",
        "VPCZoneIdentifier": ap_subnet_id,
    }
    ap_conn.create_auto_scaling_group(**group)

    len(us_conn.describe_auto_scaling_groups()["AutoScalingGroups"]).should.equal(1)
    len(ap_conn.describe_auto_scaling_groups()["AutoScalingGroups"]).should.equal(1)

    us_group = us_conn.describe_auto_scaling_groups()["AutoScalingGroups"][0]
    us_group["AutoScalingGroupName"].should.equal("us_tester_group")
    list(us_group["AvailabilityZones"]).should.equal(["us-east-1c"])
    us_group["VPCZoneIdentifier"].should.equal(us_subnet_id)
    us_group["LaunchConfigurationName"].should.equal("us_tester")
    list(us_group["LoadBalancerNames"]).should.equal(["us_test_lb"])
    us_group["PlacementGroup"].should.equal("us_test_placement")

    ap_group = ap_conn.describe_auto_scaling_groups()["AutoScalingGroups"][0]
    ap_group["AutoScalingGroupName"].should.equal("ap_tester_group")
    list(ap_group["AvailabilityZones"]).should.equal(["ap-northeast-1a"])
    ap_group["VPCZoneIdentifier"].should.equal(ap_subnet_id)
    ap_group["LaunchConfigurationName"].should.equal("ap_tester")
    list(ap_group["LoadBalancerNames"]).should.equal(["ap_test_lb"])
    ap_group["PlacementGroup"].should.equal("ap_test_placement")
