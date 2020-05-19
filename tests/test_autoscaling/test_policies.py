from __future__ import unicode_literals
import boto3
import sure  # noqa

from moto import mock_autoscaling
from utils import setup_networking


def setup_autoscale_group():
    mocked_networking = setup_networking()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester", ImageId="ami-abcd1234", InstanceType="m1.small"
    )
    conn.create_launch_configuration(**config)

    group = dict(
        AutoScalingGroupName="tester_group",
        MaxSize=2,
        MinSize=2,
        LaunchConfigurationName="tester",
        VPCZoneIdentifier=mocked_networking["subnet1"],
    )
    conn.create_auto_scaling_group(**group)
    return group


@mock_autoscaling
def test_create_policy():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
        Cooldown=60,
    )
    conn.put_scaling_policy(**policy)

    policy = conn.describe_policies()["ScalingPolicies"][0]
    policy['PolicyName'].should.equal("ScaleUp")
    policy['AdjustmentType'].should.equal("ExactCapacity")
    policy['AutoScalingGroupName'].should.equal("tester_group")
    policy['ScalingAdjustment'].should.equal(3)
    policy['Cooldown'].should.equal(60)


@mock_autoscaling
def test_create_policy_default_values():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
    )
    conn.put_scaling_policy(**policy)

    policy = conn.describe_policies()["ScalingPolicies"][0]
    policy["PolicyName"].should.equal("ScaleUp")

    # Defaults
    policy["Cooldown"].should.equal(300)


@mock_autoscaling
def test_update_policy():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
    )
    conn.put_scaling_policy(**policy)

    policy = conn.describe_policies()["ScalingPolicies"][0]
    policy["ScalingAdjustment"].should.equal(3)

    # Now update it by creating another with the same name
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=2,
    )
    conn.put_scaling_policy(**policy)
    policy = conn.describe_policies()["ScalingPolicies"][0]
    policy["ScalingAdjustment"].should.equal(2)


@mock_autoscaling
def test_delete_policy():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
    )
    conn.put_scaling_policy(**policy)

    conn.describe_policies()["ScalingPolicies"].should.have.length_of(1)

    conn.delete_policy(PolicyName="ScaleUp")
    conn.describe_policies()["ScalingPolicies"].should.have.length_of(0)


@mock_autoscaling
def test_execute_policy_exact_capacity():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ExactCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
    )
    conn.put_scaling_policy(**policy)

    conn.execute_policy(PolicyName="ScaleUp")

    instances = list(conn.describe_auto_scaling_instances()["AutoScalingInstances"])
    instances.should.have.length_of(3)


@mock_autoscaling
def test_execute_policy_positive_change_in_capacity():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="ChangeInCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=3,
    )
    conn.put_scaling_policy(**policy)

    conn.execute_policy(PolicyName="ScaleUp")

    instances = list(conn.describe_auto_scaling_instances()["AutoScalingInstances"])
    instances.should.have.length_of(5)


@mock_autoscaling
def test_execute_policy_percent_change_in_capacity():
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="PercentChangeInCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=50,
    )
    conn.put_scaling_policy(**policy)

    conn.execute_policy(PolicyName="ScaleUp")

    instances = list(conn.describe_auto_scaling_instances()["AutoScalingInstances"])
    instances.should.have.length_of(3)


@mock_autoscaling
def test_execute_policy_small_percent_change_in_capacity():
    """ http://docs.aws.amazon.com/AutoScaling/latest/DeveloperGuide/as-scale-based-on-demand.html
    If PercentChangeInCapacity returns a value between 0 and 1,
    Auto Scaling will round it off to 1."""
    setup_autoscale_group()
    conn = boto3.client("autoscaling", region_name="us-east-1")
    policy = dict(
        PolicyName="ScaleUp",
        AdjustmentType="PercentChangeInCapacity",
        AutoScalingGroupName="tester_group",
        ScalingAdjustment=1,
    )
    conn.put_scaling_policy(**policy)

    conn.execute_policy(PolicyName="ScaleUp")

    instances = list(conn.describe_auto_scaling_instances()["AutoScalingInstances"])
    instances.should.have.length_of(3)
