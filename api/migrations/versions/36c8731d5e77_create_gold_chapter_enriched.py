"""create_gold_chapter_enriched

Revision ID: 36c8731d5e77
Revises:
Create Date: 2026-05-07 20:54:53.626990

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "36c8731d5e77"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS exercise_book_gold")
    op.create_table(
        "gold_chapter_enriched",
        sa.Column("chapter_id", sa.String(), nullable=False),
        sa.Column("book_id", sa.String(), nullable=False),
        sa.Column("book_title", sa.String(), nullable=True),
        sa.Column("chapter_order", sa.Integer(), nullable=True),
        sa.Column("source_language", sa.String(), nullable=True),
        sa.Column("category", sa.String(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("a2_text", sa.Text(), nullable=True),
        sa.Column("a2_summary", sa.Text(), nullable=True),
        sa.Column("b1b2_text", sa.Text(), nullable=True),
        sa.Column("swedish_text", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("chapter_id"),
        schema="exercise_book_gold",
    )
    op.create_index(
        "ix_gold_chapter_enriched_book_id",
        "gold_chapter_enriched",
        ["book_id"],
        schema="exercise_book_gold",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_gold_chapter_enriched_book_id",
        table_name="gold_chapter_enriched",
        schema="exercise_book_gold",
    )
    op.drop_table("gold_chapter_enriched", schema="exercise_book_gold")
