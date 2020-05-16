from __future__ import unicode_literals

import boto3
import sure  # noqa

from moto import mock_glacier


@mock_glacier
def test_create_vault():
    conn = boto3.client("glacier", region_name="us-west-2")

    conn.create_vault(vaultName="my_vault")

    vaults = conn.list_vaults()['VaultList']
    vaults.should.have.length_of(1)
    vaults[0]['VaultName'].should.equal("my_vault")


@mock_glacier
def test_delete_vault():
    conn = boto3.client("glacier", region_name="us-west-2")

    conn.create_vault(vaultName="my_vault")

    vaults = conn.list_vaults()['VaultList']
    vaults.should.have.length_of(1)

    conn.delete_vault(vaultName="my_vault")
    vaults = conn.list_vaults()['VaultList']
    vaults.should.have.length_of(0)
