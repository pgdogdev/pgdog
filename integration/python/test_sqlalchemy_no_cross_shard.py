#!/usr/bin/env python3
"""
SQLAlchemy connection pool example with PostgreSQL-specific configuration.
Sets search_path on connection creation and work_mem within a transaction.
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

# Database connection URL (using psycopg3 driver)
DATABASE_URL = "postgresql+psycopg://pgdog:pgdog@127.0.0.1:6432/pgdog"



def main():
    # Create engine with connection pool
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        echo=True  # Set to False in production
    )

    # Register event listener to set search_path on connect
    @event.listens_for(engine, "connect", insert=True)
    def set_search_path(dbapi_conn, connection_record):
        """
        Event listener to set search_path when a connection is first created.
        Temporarily enables autocommit to ensure the setting is applied immediately.
        """

        # Save the current autocommit state
        old_autocommit = dbapi_conn.autocommit

        try:
            # Temporarily set autocommit to True
            dbapi_conn.autocommit = True
            cursor = dbapi_conn.cursor()
            cursor.execute("SET search_path TO acustomer, public, pg_catalog")
            cursor.close()
        finally:
            # Restore original autocommit state
            dbapi_conn.autocommit = old_autocommit



    def get_session():
        return Session(engine)

    # Create a session (which starts a transaction)
    with get_session() as session, session.begin():
        session.execute(text("SET LOCAL work_mem TO '256MB'"))


if __name__ == "__main__":
    main()
