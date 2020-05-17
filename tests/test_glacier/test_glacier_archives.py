from __future__ import unicode_literals

from tempfile import NamedTemporaryFile
import boto3
import sure  # noqa

from moto import mock_glacier


@mock_glacier
def test_create_and_delete_archive():
    vault_name = "my_vault"

    conn = boto3.client("glacier", region_name="us-west-2")
    _ = conn.create_vault(vaultName=vault_name)

    archive_id = conn.upload_archive(vaultName=vault_name, body=b"some stuff")[
        "archiveId"
    ]

    conn.delete_archive(vaultName=vault_name, archiveId=archive_id)
