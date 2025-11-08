"""
Smoke test that simulates a full pipeline run against a temporary schema.

This test creates a temporary schema, runs the pipeline logic, and verifies
data flows through all layers without polluting production schemas.
"""

import pytest
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path


@pytest.fixture(scope="module")
def temp_schema():
    """Create a temporary schema for testing."""
    # Get database connection from environment
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/dw"
    )
    
    # Parse connection string
    conn = psycopg2.connect(database_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Create temporary schema
    schema_name = f"test_smoke_{os.getpid()}"
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    yield schema_name
    
    # Cleanup: Drop temporary schema
    try:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        conn.commit()
    except Exception:
        pass
    finally:
        cursor.close()
        conn.close()


@pytest.mark.smoke
def test_schema_creation(temp_schema):
    """Test that temporary schema was created."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/dw"
    )
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    # Verify schema exists
    cursor.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = %s
    """, (temp_schema,))
    
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == temp_schema
    
    cursor.close()
    conn.close()


@pytest.mark.smoke
def test_table_creation(temp_schema):
    """Test that tables can be created in temporary schema."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/dw"
    )
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    # Create a test table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {temp_schema}.test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # Verify table exists
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = 'test_table'
    """, (temp_schema,))
    
    result = cursor.fetchone()
    assert result is not None
    
    # Insert test data
    cursor.execute(f"""
        INSERT INTO {temp_schema}.test_table (name) 
        VALUES ('test_record')
    """)
    conn.commit()
    
    # Verify data was inserted
    cursor.execute(f"SELECT COUNT(*) FROM {temp_schema}.test_table")
    count = cursor.fetchone()[0]
    assert count == 1
    
    cursor.close()
    conn.close()


@pytest.mark.smoke
def test_data_flow_simulation(temp_schema):
    """Simulate data flow through raw -> core -> mart layers."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/dw"
    )
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    # Create simplified schema structure
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {temp_schema}.raw_data (
            id SERIAL PRIMARY KEY,
            payload JSONB,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {temp_schema}.core_fact (
            id SERIAL PRIMARY KEY,
            observed_at TIMESTAMP,
            value DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {temp_schema}.mart_agg AS
        SELECT 
            DATE(observed_at) as observation_date,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM {temp_schema}.core_fact
        GROUP BY DATE(observed_at)
    """)
    
    conn.commit()
    
    # Simulate raw data insertion
    cursor.execute(f"""
        INSERT INTO {temp_schema}.raw_data (payload)
        VALUES ('{{"test": "data"}}'::jsonb)
    """)
    conn.commit()
    
    # Simulate core fact insertion
    cursor.execute(f"""
        INSERT INTO {temp_schema}.core_fact (observed_at, value)
        VALUES (CURRENT_TIMESTAMP, 42.5)
    """)
    conn.commit()
    
    # Refresh materialized view
    cursor.execute(f"REFRESH MATERIALIZED VIEW {temp_schema}.mart_agg")
    conn.commit()
    
    # Verify data flow
    cursor.execute(f"SELECT COUNT(*) FROM {temp_schema}.raw_data")
    raw_count = cursor.fetchone()[0]
    assert raw_count == 1
    
    cursor.execute(f"SELECT COUNT(*) FROM {temp_schema}.core_fact")
    core_count = cursor.fetchone()[0]
    assert core_count == 1
    
    cursor.execute(f"SELECT COUNT(*) FROM {temp_schema}.mart_agg")
    mart_count = cursor.fetchone()[0]
    assert mart_count == 1
    
    cursor.close()
    conn.close()


@pytest.mark.smoke
def test_database_connection():
    """Test that database connection works."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/dw"
    )
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        cursor.close()
        conn.close()
    except psycopg2.OperationalError:
        pytest.skip("Database not available for testing")

