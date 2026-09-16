# Frappe application commands

Applications used by the Docker stack should be included in the common image,
not downloaded independently into running containers. This ensures the
backend, frontend, scheduler, and workers all execute identical code.

After adding an application to the image definition, rebuild and recreate the
application services:

```bash
docker compose -f ERPNext/docker-compose.yml build backend
docker compose -f ERPNext/docker-compose.yml up -d --no-deps --force-recreate backend frontend queue-long queue-short scheduler websocket
```

Install and migrate an application already present in the image:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend install-app APP_NAME
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend migrate
```

Verify installed applications:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend list-apps
```

Uninstall an application from the site:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend uninstall-app APP_NAME
```

Uninstalling does not remove the application from the image. Change the image
definition and rebuild if its code must also be removed.
