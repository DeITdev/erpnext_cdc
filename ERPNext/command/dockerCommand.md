# ERPNext Docker commands

The checked-in stack uses `ERPNext/docker-compose.yml` and builds the custom
ERPNext/HRMS image when needed.

Start the stack from the repository root:

```bash
docker compose -f ERPNext/docker-compose.yml up -d --build
```

Inspect container status and site-creation logs:

```bash
docker compose -f ERPNext/docker-compose.yml ps
docker compose -f ERPNext/docker-compose.yml logs -f create-site
```

Stop the stack without deleting persistent data:

```bash
docker compose -f ERPNext/docker-compose.yml down
```

Do not add `--volumes` unless the exact database and site volumes have been
backed up and intentionally need to be deleted.
