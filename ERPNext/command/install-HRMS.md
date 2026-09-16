# Frappe HR installation and maintenance

HRMS is built into `ERPNext/Containerfile` at the release-aligned `v16.7.1`
tag. Do not run `bench get-app hrms` in a running container: its default branch
is `develop`, which currently requires Frappe/ERPNext v17.

## Build and deploy

From the repository root:

```bash
docker compose -f ERPNext/docker-compose.yml build backend
docker compose -f ERPNext/docker-compose.yml up -d --no-deps --force-recreate backend frontend queue-long queue-short scheduler websocket
```

Install HRMS on a site that does not already have it, then migrate:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend install-app hrms
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend migrate
```

For an existing installation, run only `migrate` after deploying the rebuilt
image.

## Refresh navigation and icons

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend clear-cache
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend clear-website-cache
```

Sign out and back in, or perform a hard browser refresh, after clearing cache.

## Repair stale workspace sidebar metadata

If the site previously ran an incompatible HRMS branch, newer database copies of
the standard sidebar records can survive a downgrade and override the correct
module mapping from the pinned release. Affected HR users may then see only the
Payroll workspace. Back up the site, re-import the standard HRMS sidebars, and
clear the navigation cache:

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend backup --with-files
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend import-doc apps/hrms/hrms/workspace_sidebar
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend clear-cache
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend clear-website-cache
```

The release maps Expenses, HR Setup, Leaves, Performance, Recruitment, Shift &
Attendance, and Tenure to the `HR` module. Payroll and Tax & Benefits map to the
`Payroll` module.

## Verify

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend list-apps
docker compose -f ERPNext/docker-compose.yml ps
curl -I http://127.0.0.1:8080/hrms
```

## Uninstall

```bash
docker compose -f ERPNext/docker-compose.yml exec -T backend bench --site frontend uninstall-app hrms
```

The application remains part of the custom image after a site uninstall.
Remove it from `ERPNext/Containerfile` and rebuild only if its code must be
removed from every service.
