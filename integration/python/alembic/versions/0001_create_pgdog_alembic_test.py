"""Create the Alembic integration test table."""

from alembic import op
import sqlalchemy as sa


revision = "0001_pgdog_alembic"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "alembic_pgdog_test",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
    )


def downgrade():
    op.drop_table("alembic_pgdog_test")
