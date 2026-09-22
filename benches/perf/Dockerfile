# Re-uses the base runtime image; injects this branch's built pgdog binary and
# the perf scripts we're gonna run in Bencher.

FROM ghcr.io/pgdogdev/pgdog-base-runtime:latest

RUN apt update && apt install -y --no-install-recommends postgresql-18 sudo iproute2

WORKDIR /pgdog
COPY --from=bin pgdog /usr/local/bin/pgdog
COPY integration/common.sh integration/setup.sh integration/
COPY integration/ci/setup.sh integration/ci/
COPY pgdog/src/backend/schema/setup.sql pgdog/src/backend/schema/
COPY benches/perf benches/perf

# This happens here because we don't have network access in Bencher.
RUN bash integration/ci/setup.sh && pg_ctlcluster 18 main stop

CMD ["bash", "benches/perf/bencher.sh"]
