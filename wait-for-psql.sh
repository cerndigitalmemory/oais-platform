#!/bin/sh
# wait-for-postgres.sh
# This script "holds" the execution of the given command until
# a postgres instances is found on the specified location
# (the postgres client needs to be installed)

set -e
  
host="$1"
shift
  
until PGPASSWORD=$DB_PASS psql -h "$host" -U "postgres" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Postgres is up - executing command"
exec "$@"
