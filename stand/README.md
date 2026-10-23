# Kerberos stand

Files copied unchanged from the ldap-kdc repository:

- `ldap-kdc/bootstrap.yaml` — the demo directory (from `compose/`);
- `krb5.conf` — the client configuration (from `compose/`);
- `openbao/` — the OpenBao image's build context (from `test/e2e/testdata/`).

`ldap-kdc/ldap-kdc.yaml` is the KDC's configuration from `compose/`, with `dns.docker` turned on:
the KDC registers every container of the `example.com` network in its zone (`consul.example.com`,
`tembo-a.example.com`, ...) and answers reverse lookups from those records.

`resolv.conf` points the containers at the KDC: they resolve the zone's names and get PTR answers
from it. Docker's embedded resolver is bypassed for that reason, as in ldap-kdc's e2e stand.

The KDC is `ghcr.io/shulutkov/ldap-kdc:1.11.0`, pulled, not built. The KDC's Docker socket is
read through the socket's group (`group_add: "0"` on Docker Desktop).

Run with `docker compose -f docker-compose.yaml -f docker-compose.ext.yaml up -d --build`.

The scripts in this directory (`node-start.sh`, `openbao-init.sh`, `kdc-init.sh`) are the stand's own.
