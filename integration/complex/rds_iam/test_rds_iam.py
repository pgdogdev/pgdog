from urllib.parse import parse_qs, urlsplit

import boto3
import psycopg
import pytest
from botocore.exceptions import NoCredentialsError


@pytest.fixture(scope="module")
def rds_iam_token():
    """Generate a staging-iam token using the default AWS credential chain."""
    hostname = "staging-iam.cluster-c5icciqq4b0q.us-west-2.rds.amazonaws.com"
    client = boto3.client("rds", region_name="us-west-2")

    try:
        return client.generate_db_auth_token(
            DBHostname=hostname,
            Port=5432,
            DBUsername="postgres",
            Region="us-west-2",
        )
    except NoCredentialsError:
        pytest.skip("AWS credentials are required to generate an RDS IAM token")


def test_generate_rds_iam_token(rds_iam_token):
    url = urlsplit(f"https://{rds_iam_token}")
    params = parse_qs(url.query)
    assert (
        url.hostname == "staging-iam.cluster-c5icciqq4b0q.us-west-2.rds.amazonaws.com"
    )
    assert url.port == 5432
    assert params["Action"] == ["connect"]
    assert params["DBUser"] == ["postgres"]
    assert params["X-Amz-Expires"] == ["900"]
    assert params["X-Amz-Credential"][0].endswith("/us-west-2/rds-db/aws4_request")
    assert params["X-Amz-Signature"][0]


def test_connect_to_pgdog(rds_iam_token):
    """Reuse one IAM token across ten separate client connections."""
    for attempt in range(10):
        with psycopg.connect(
            host="127.0.0.1",
            port=6432,
            dbname="postgres",
            user="postgres",
            password=rds_iam_token,
            sslmode="disable",
            connect_timeout=10,
            options="-c statement_timeout=10000",
        ) as conn:
            row = conn.execute("SELECT current_user, current_database()").fetchone()
            assert row == ("postgres", "postgres"), f"Connection {attempt + 1}"
