# Blockchain HCM application

The `blockchain_hcm` application is not part of the current common image. Add
its repository and an immutable compatible revision to the image definition
before installing it on the site. Do not run `bench get-app` independently in
running containers, because workers and the scheduler would not receive the
same code.

After the image contains the application:

```bash
docker compose -f ERPNext/docker-compose.yml build backend
docker compose -f ERPNext/docker-compose.yml up -d --no-deps --force-recreate backend frontend queue-long queue-short scheduler websocket
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend install-app blockchain_hcm
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend migrate
```

Uninstall it from the site with:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend uninstall-app blockchain_hcm
```
