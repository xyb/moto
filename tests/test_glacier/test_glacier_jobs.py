from __future__ import unicode_literals

import json
import time

import boto3
import sure  # noqa

from moto import mock_glacier


@mock_glacier
def test_init_glacier_job():
    conn = boto3.client("glacier", region_name="us-west-2")
    vault_name = "my_vault"
    conn.create_vault(vaultName=vault_name)
    archive_id = conn.upload_archive(
        vaultName=vault_name,
        body="some stuff",
        checksum="",
        archiveDescription="some description",
    )["archiveId"]

    job_response = conn.initiate_job(
        vaultName=vault_name,
        jobParameters={"ArchiveId": archive_id, "Type": "archive-retrieval"},
    )
    job_id = job_response["jobId"]
    job_response["location"].should.contain("//vaults/my_vault/jobs/{0}".format(job_id))


@mock_glacier
def test_describe_job():
    conn = boto3.client("glacier", region_name="us-east-1")
    vault_name = "my_vault"
    conn.create_vault(vaultName=vault_name)
    archive_id = conn.upload_archive(
        vaultName=vault_name,
        body="some stuff",
        checksum="",
        archiveDescription="some description",
    )["archiveId"]
    job_response = conn.initiate_job(
        vaultName=vault_name,
        jobParameters={"ArchiveId": archive_id, "Type": "archive-retrieval"},
    )
    job_id = job_response["jobId"]

    job = conn.describe_job(vaultName=vault_name, jobId=job_id)

    job.should.have.key("Tier").which.should.equal("Standard")
    job.should.have.key("StatusCode").which.should.equal("InProgress")
    job.should.have.key("VaultARN").which.should.equal(
        "arn:aws:glacier:us-east-1:012345678901:vaults/my_vault"
    )


@mock_glacier
def test_list_glacier_jobs():
    conn = boto3.client("glacier", region_name="us-west-2")
    vault_name = "my_vault"
    conn.create_vault(vaultName=vault_name)
    archive_id1 = conn.upload_archive(
        vaultName=vault_name,
        body="some stuff",
        checksum="",
        archiveDescription="some description",
    )["archiveId"]
    archive_id2 = conn.upload_archive(
        vaultName=vault_name,
        body="some other stuff",
        checksum="",
        archiveDescription="some description",
    )["archiveId"]

    conn.initiate_job(
        vaultName=vault_name,
        jobParameters={"ArchiveId": archive_id1, "Type": "archive-retrieval"},
    )
    conn.initiate_job(
        vaultName=vault_name,
        jobParameters={"ArchiveId": archive_id2, "Type": "archive-retrieval"},
    )

    jobs = conn.list_jobs(vaultName=vault_name)
    len(jobs["JobList"]).should.equal(2)


@mock_glacier
def test_get_job_output():
    conn = boto3.client("glacier", region_name="us-west-2")
    vault_name = "my_vault"
    conn.create_vault(vaultName=vault_name)
    archive_response = conn.upload_archive(
        vaultName=vault_name,
        body="some stuff",
        checksum="",
        archiveDescription="some description",
    )
    archive_id = archive_response["archiveId"]
    job_response = conn.initiate_job(
        vaultName=vault_name,
        jobParameters={"ArchiveId": archive_id, "Type": "archive-retrieval"},
    )
    job_id = job_response["jobId"]

    time.sleep(6)

    output = conn.get_job_output(vaultName=vault_name, jobId=job_id)["body"]
    output.read().decode("utf-8").should.equal("some stuff")
