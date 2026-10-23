#!/bin/sh
# Initializes the stand's OpenBao (auto-unsealed against SoftHSM), enables KV v2 at secret/ and
# writes the Postgres password the nodes read. The root token goes to /bao/root-token for the
# nodes. The server is in memory: a fresh start initializes it again.
set -eu
export BAO_ADDR=http://openbao:8200

# bao status exits 1 only when it cannot reach the server; 0 and 2 mean it answers.
while :; do
	rc=0
	bao status >/dev/null 2>&1 || rc=$?
	[ "$rc" -ne 1 ] && break
	sleep 1
done

if bao status -format=json | grep -q '"initialized": *true'; then
	echo "openbao: already initialized, reusing /bao/root-token"
else
	bao operator init -recovery-shares=1 -recovery-threshold=1 -format=json > /bao/init.json
	sed -n 's/.*"root_token": *"\([^"]*\)".*/\1/p' /bao/init.json > /bao/root-token
	echo "openbao: initialized"
fi
chmod 644 /bao/root-token

# With an auto-seal the server unseals itself; exit code 0 means unsealed.
until bao status >/dev/null 2>&1; do sleep 1; done

export BAO_TOKEN
BAO_TOKEN=$(cat /bao/root-token)
bao secrets list -format=json | grep -q '"secret/"' || bao secrets enable -path=secret kv-v2
bao kv put secret/tembo/pg password="${TEMBO_PG_PASSWORD}" >/dev/null
echo "openbao: secret/tembo/pg written"
