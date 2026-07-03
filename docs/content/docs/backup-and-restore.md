---
title: "Backup and restore"
weight: 29
---

The runtime can export all of its persistent data to a portable file and load it back later, using the `backup` and `restore` subcommands. This is useful for disaster recovery, cloning an environment, and migrating between data stores/providers.

A backup contains everything the provider persists:

- Actor state
- Alarms, including scheduled [jobs](/docs/jobs)
- Dead-lettered jobs

Ephemeral data (e.g. host registrations and alarm leases) is intentionally left out, since it's recreated automatically once hosts reconnect.

The format is provider-neutral, so a backup taken from one provider can be restored into a different one (for example, from SQLite into PostgreSQL)

Both subcommands discover their configuration the [same way the runtime does](/docs/deploying-the-runtime#config-file-location) (the `FRANCIS_CONFIG` environment variable, then the well-known paths) and connect to the data store named by `provider.connectionString`.

## Creating a backup

```sh
francis backup -f backup.francis
```

The `-f` flag is required and names the destination file. Use `-f -` to write the backup to _stdout_ instead, which lets you pipe and/or compress it:

```sh
# Compress on the fly
francis backup -f - | gzip > backup.francis.gz
```

Taking a backup is safe against a live (running) cluster.

When running from a container, invoke the subcommand inside it with the config and data store mounted, and redirect stdout on the host:

```sh
docker run --rm \
    -v "$(pwd)/config.yaml:/etc/francis/config.yaml:ro" \
    -v francis-data:/data \
    ghcr.io/italypaleale/francis:1 \
    backup -f - \
  > backup.francis
```

## Restoring a backup

Restore wipes all existing persistent data and then loads the snapshot:

```sh
francis restore -f backup.francis
```

Use `-f -` to read from _stdin_:

```sh
gunzip -c backup.francis.gz | francis restore -f -
```

Restore also works against an empty database.

> Note: **Restore refuses to run while hosts are connected**  
> Restoring underneath live hosts would corrupt running actors, so `restore` checks the target database for connected hosts first. If any host has reported a health check within the [`healthCheckDeadline`](/docs/deploying-the-runtime#configuration-reference), the command exits with a non-zero status and changes nothing. Stop every host and any runtime replica sharing the database before restoring.

From a container, pass `-i` so stdin is forwarded:

```sh
cat backup.francis \
  | docker run --rm -i \
    -v "$(pwd)/config.yaml:/etc/francis/config.yaml:ro" \
    -v francis-data:/data \
    ghcr.io/italypaleale/francis:1 \
    restore -f -
```

## Migrating between data stores

Because the backup format is provider-neutral, you can move data between backends by backing up with one connection string and restoring with another. For example, to migrate from SQLite to PostgreSQL:

```sh
# Export from the current (SQLite) store
FRANCIS_CONFIG=./sqlite-config.yaml francis backup -f - \
  | FRANCIS_CONFIG=./postgres-config.yaml francis restore -f -
```

> Note: The in-memory provider can't be backed up from the CLI.
