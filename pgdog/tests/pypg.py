import psycopg2
import asyncpg
import asyncio

async def test_asyncpg():
	conn = await asyncpg.connect(
		user='pgdog',
		password='pgdog',
		database='pgdog',
		host='127.0.0.1',
		port=6432)
	for i in range(100):
		values = await conn.fetch("SELECT $1::int, $2::text", 1, "1")
	await conn.close()

asyncio.run(test_asyncpg())
