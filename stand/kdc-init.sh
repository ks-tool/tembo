#!/bin/sh
# Creates the stand's Kerberos principals in ldap-kdc and writes the keytabs. Each node gets one
# keytab for its own API principal (HTTP/), its Postgres service (postgres/) and the agent's own
# principal (postgres, which maps to the superuser); alice gets hers for the shell test. Keytabs come from POST /api/v1/keytab,
# which answers one file for a whole list of principals.
set -eu
API=http://ldap-kdc:5555
AUTH="Authorization: Bearer compose-token"

until curl -fs -o /dev/null "$API/readyz"; do sleep 2; done

create() { # create <json>: 201 for a new principal, 409 when it exists already
	code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/principals" \
		-H "$AUTH" -H 'Content-Type: application/json' -d "$1")
	case "$code" in
		201|409) ;;
		*) echo "kdc-init: creating principal failed: HTTP $code: $1" >&2; exit 1 ;;
	esac
}

keytab() { # keytab <json-list-of-principals> <file>
	curl -fs -X POST "$API/api/v1/keytab" -H "$AUTH" -H 'Content-Type: application/json' \
		-d "{\"principals\": $1}" -o "$2"
}

create '{"name":"postgres"}'
for n in a b c; do
	fqdn="tembo-$n.example.com"
	create "{\"name\":\"HTTP/$fqdn\",\"okToAuthAsDelegate\":true,\"allowedToDelegateTo\":[\"postgres/$fqdn\"],\"allowedToImpersonate\":[\"alice@EXAMPLE.COM\"]}"
	create "{\"name\":\"postgres/$fqdn\"}"
	keytab "[\"HTTP/$fqdn\", \"postgres/$fqdn\", \"postgres\"]" "/out/tembo-$n/node.keytab"
	echo "kdc-init: $fqdn keytab written"
done

keytab '["alice@EXAMPLE.COM"]' /out/client/alice.keytab
echo "kdc-init: alice keytab written"
