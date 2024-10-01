# Sqlite backend

## Adding a migration

1. Install [golang-migrate/migrate](https://www.github.com/golang-migrate/migrate)
1. ```bash
   migrate create -ext sql -dir ./db/migrations -seq <name>
   ```