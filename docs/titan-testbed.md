# Titan multi-user testbed

The Titan testbed qualifies remote synchronization without reading or mounting the
personal archive. It runs in the `convos-testbed` LXD container, stores only
synthetic conversations, and has separate destructive-fresh and persistent-canary
lanes.

## Isolation and layout

- LXD container: `convos-testbed`, Ubuntu minimal 24.04, macvlan on `eno1`.
- Dedicated storage: `/home/robert/.local/share/convos-testbed/lxd` on Titan.
- Relay account: `convos-relay`.
- Fresh users: `convos-fresh-a` and `convos-fresh-b`.
- Canary users: `convos-canary-a` and `convos-canary-b`.
- Relay and evidence state: `/var/lib/convos-testbed/{fresh,canary,evidence}`.
- Current checkout environment: `/opt/convos/wip`.
- Released comparison environment: `/opt/convos/released-v0.10.1`.

The lane directories are mode `0700`. No host home, Convos archive, browser
profile, provider credential, or recovery key is mounted into the container.

## Provisioning

Run these commands on Titan. The storage path and network parent are intentionally
explicit; change them for another host.

```sh
lxc remote add ubuntu-minimal https://cloud-images.ubuntu.com/minimal/releases --protocol=simplestreams
lxc storage create convos-testbed dir source=/home/robert/.local/share/convos-testbed/lxd
lxc launch ubuntu-minimal:24.04 convos-testbed --storage convos-testbed
lxc config device override convos-testbed eth0 nictype=macvlan parent=eno1
lxc restart convos-testbed
lxc exec convos-testbed -- apt-get update
lxc exec convos-testbed -- apt-get install -y ca-certificates curl git python3-venv build-essential
lxc exec convos-testbed -- sh -c 'curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh'
for user in convos-relay convos-fresh-a convos-fresh-b convos-canary-a convos-canary-b; do
  lxc exec convos-testbed -- useradd -m -s /bin/bash "$user"
done
lxc exec convos-testbed -- install -d -m 0700 -o convos-relay -g convos-relay /var/lib/convos-testbed/fresh /var/lib/convos-testbed/canary /var/lib/convos-testbed/evidence
```

Deploy an exact checkout and install the core, client, and relay server into one
environment. Keep the released environment immutable after installing its tag.

```sh
commit=$(git rev-parse HEAD)
ssh titan "lxc exec convos-testbed -- mkdir -p /opt/convos/source-$commit"
git archive "$commit" | ssh titan "lxc exec convos-testbed -- tar -x -C /opt/convos/source-$commit"
ssh titan "lxc exec convos-testbed -- uv venv /opt/convos/wip"
ssh titan "lxc exec convos-testbed -- uv pip install --python /opt/convos/wip/bin/python /opt/convos/source-$commit /opt/convos/source-$commit/apps/remote /opt/convos/source-$commit/apps/remote_server"
```

## Qualification lanes

The fresh lane deletes only the fixed fresh users' lane roots and fresh relay DB.
It enrolls two users and three devices, synchronizes personal and team archives,
uses different checkout paths for the same repository, applies concurrent updates,
removes a recovered device, restarts and restores the relay, and requires an
idempotent second sync.

```sh
commit=$(git rev-parse HEAD)
ssh titan "lxc exec convos-testbed -- /opt/convos/wip/bin/python /opt/convos/source-$commit/scripts/remote_testbed.py fresh --venv /opt/convos/wip --commit $commit"
```

The canary lane never resets its homes, archives, relay, manifest, or migration
backups. Its first run bootstraps with the released environment, requires a
released client to ingest current signed-v1 rows, then upgrades that client. Later
runs append another current-checkout conversation and history entry.

```sh
released_commit=$(git rev-parse v0.10.1)
current_commit=$(git rev-parse HEAD)
ssh titan "lxc exec convos-testbed -- /opt/convos/wip/bin/python /opt/convos/source-$current_commit/scripts/remote_testbed.py canary --released-venv /opt/convos/released-v0.10.1 --current-venv /opt/convos/wip --released-commit $released_commit --current-commit $current_commit"
```

Each successful run writes a mode-`0600` JSON record under
`/var/lib/convos-testbed/evidence`. Evidence includes exact commits and package
versions, semantic archive hashes and counts, conflicts, input outcomes, harness
retries and failures, elapsed time, peak child RSS, relay row count, doctor output,
migration backups, and the plaintext-canary result.

## Known clean-install finding

Installing Convos plus the remote products in a clean Ubuntu 24.04 container did
not succeed before `build-essential` was installed. `llama-cpp-python==0.3.35`
fell back to a local source build, so the current distribution has an undocumented
compiler/toolchain requirement and incurred a large package installation plus an
approximately 68-second native build. This is a stable-release blocker until the
installation contract is deliberately fixed or documented and tested.
