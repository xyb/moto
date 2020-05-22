from __future__ import unicode_literals

import base64
import boto3
import sure  # noqa

from moto import mock_autoscaling
from moto.core import ACCOUNT_ID
from tests.helpers import requires_boto_gte


@mock_autoscaling
def test_create_launch_configuration():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        InstanceType="t1.micro",
        KeyName="the_keys",
        SecurityGroups=["default", "default2"],
        UserData=b"This is some user_data",
        InstanceMonitoring={"Enabled": True},
        IamInstanceProfile="arn:aws:iam::{}:instance-profile/testing".format(
            ACCOUNT_ID
        ),
        SpotPrice="0.1",
    )
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["LaunchConfigurationName"].should.equal("tester")
    launch_config["ImageId"].should.equal("ami-abcd1234")
    launch_config["InstanceType"].should.equal("t1.micro")
    launch_config["KeyName"].should.equal("the_keys")
    set(launch_config["SecurityGroups"]).should.equal(set(["default", "default2"]))
    base64.decodestring(launch_config["UserData"].encode("utf8")).should.equal(
        b"This is some user_data"
    )
    launch_config["InstanceMonitoring"]["Enabled"].should.equal(True)
    launch_config["IamInstanceProfile"].should.equal(
        "arn:aws:iam::{}:instance-profile/testing".format(ACCOUNT_ID)
    )
    launch_config["SpotPrice"].should.equal("0.1")


@mock_autoscaling
def test_create_launch_configuration_with_block_device_mappings():

    block_device_mappings = [
        {"VirtualName": "ephemeral0", "DeviceName": "/dev/xvdb",},
        {
            "DeviceName": "/dev/xvdp",
            "Ebs": {"SnapshotId": "snap-1234abcd", "VolumeType": "standard",},
        },
        {
            "DeviceName": "/dev/xvdh",
            "Ebs": {
                "VolumeSize": 100,
                "VolumeType": "io1",
                "DeleteOnTermination": False,
                "Iops": 1000,
            },
        },
    ]

    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        InstanceType="m1.small",
        KeyName="the_keys",
        SecurityGroups=["default", "default2"],
        UserData=b"This is some user_data",
        InstanceMonitoring={"Enabled": True},
        IamInstanceProfile="arn:aws:iam::{}:instance-profile/testing".format(
            ACCOUNT_ID
        ),
        SpotPrice="0.1",
        BlockDeviceMappings=block_device_mappings,
    )
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["LaunchConfigurationName"].should.equal("tester")
    launch_config["ImageId"].should.equal("ami-abcd1234")
    launch_config["InstanceType"].should.equal("m1.small")
    launch_config["KeyName"].should.equal("the_keys")
    set(launch_config["SecurityGroups"]).should.equal(set(["default", "default2"]))
    base64.decodestring(launch_config["UserData"].encode("utf8")).should.equal(
        b"This is some user_data"
    )
    launch_config["InstanceMonitoring"]["Enabled"].should.equal(True)
    launch_config["IamInstanceProfile"].should.equal(
        "arn:aws:iam::{}:instance-profile/testing".format(ACCOUNT_ID)
    )
    launch_config["SpotPrice"].should.equal("0.1")
    len(launch_config["BlockDeviceMappings"]).should.equal(3)

    returned_mapping = launch_config["BlockDeviceMappings"]

    devices = {block["DeviceName"]: block for block in returned_mapping}
    set(devices.keys()).should.equal(set(["/dev/xvdb", "/dev/xvdp", "/dev/xvdh"]))

    devices["/dev/xvdh"]["Ebs"]["Iops"].should.equal(1000)
    devices["/dev/xvdh"]["Ebs"]["VolumeSize"].should.equal(100)
    devices["/dev/xvdh"]["Ebs"]["VolumeType"].should.equal("io1")
    devices["/dev/xvdh"]["Ebs"]["DeleteOnTermination"].should.be.false

    devices["/dev/xvdp"]["Ebs"]["SnapshotId"].should.equal("snap-1234abcd")
    devices["/dev/xvdp"]["Ebs"]["VolumeType"].should.equal("standard")

    devices["/dev/xvdb"]["VirtualName"].should.equal("ephemeral0")


@mock_autoscaling
def test_create_launch_configuration_for_2_12():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester", ImageId="ami-abcd1234", EbsOptimized=True
    )
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["EbsOptimized"].should.equal(True)


@mock_autoscaling
def test_create_launch_configuration_using_ip_association():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        AssociatePublicIpAddress=True,
    )
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["AssociatePublicIpAddress"].should.equal(True)


@mock_autoscaling
def test_create_launch_configuration_using_ip_association_should_default_to_false():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(LaunchConfigurationName="tester", ImageId="ami-abcd1234")
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["AssociatePublicIpAddress"].should.equal(False)


@mock_autoscaling
def test_create_launch_configuration_defaults():
    """ Test with the minimum inputs and check that all of the proper defaults
    are assigned for the other attributes """
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        InstanceType="m1.small",
    )
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["LaunchConfigurationName"].should.equal("tester")
    launch_config["ImageId"].should.equal("ami-abcd1234")
    launch_config["InstanceType"].should.equal("m1.small")

    # Defaults
    launch_config["KeyName"].should.equal("")
    list(launch_config["SecurityGroups"]).should.equal([])
    launch_config["UserData"].should.equal("")
    launch_config["InstanceMonitoring"]["Enabled"].should.equal(False)
    launch_config.get("IamInstanceProfile").should.equal(None)
    launch_config.get("SpotPrice").should.equal(None)


@mock_autoscaling
def test_create_launch_configuration_defaults_for_2_12():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(LaunchConfigurationName="tester", ImageId="ami-abcd1234")
    conn.create_launch_configuration(**config)

    launch_config = conn.describe_launch_configurations()["LaunchConfigurations"][0]
    launch_config["EbsOptimized"].should.equal(False)


@mock_autoscaling
def test_launch_configuration_describe_filter():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        InstanceType="m1.small",
    )
    conn.create_launch_configuration(**config)
    config["LaunchConfigurationName"] = "tester2"
    conn.create_launch_configuration(**config)
    config["LaunchConfigurationName"] = "tester3"
    conn.create_launch_configuration(**config)

    conn.describe_launch_configurations(LaunchConfigurationNames=["tester", "tester2"])[
        "LaunchConfigurations"
    ].should.have.length_of(2)
    conn.describe_launch_configurations()["LaunchConfigurations"].should.have.length_of(
        3
    )


@mock_autoscaling
def test_launch_configuration_describe_paginated():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    for i in range(51):
        conn.create_launch_configuration(LaunchConfigurationName="TestLC%d" % i)

    response = conn.describe_launch_configurations()
    lcs = response["LaunchConfigurations"]
    marker = response["NextToken"]
    lcs.should.have.length_of(50)
    marker.should.equal(lcs[-1]["LaunchConfigurationName"])

    response2 = conn.describe_launch_configurations(NextToken=marker)

    lcs.extend(response2["LaunchConfigurations"])
    lcs.should.have.length_of(51)
    assert "NextToken" not in response2.keys()


@mock_autoscaling
def test_launch_configuration_delete():
    conn = boto3.client("autoscaling", region_name="us-east-1")
    config = dict(
        LaunchConfigurationName="tester",
        ImageId="ami-abcd1234",
        InstanceType="m1.small",
    )
    conn.create_launch_configuration(**config)

    conn.describe_launch_configurations()["LaunchConfigurations"].should.have.length_of(
        1
    )

    conn.delete_launch_configuration(LaunchConfigurationName="tester")
    conn.describe_launch_configurations()["LaunchConfigurations"].should.have.length_of(
        0
    )
