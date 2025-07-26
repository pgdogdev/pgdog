.PHONY: dev connect reload

dev:
	bash integration/dev-server.sh

connect:
	psql postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded

reload:
	 pkill -HUP pgdog
