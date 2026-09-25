# Git repository identity

Convos derives repository evidence from Git lineage and normalized fetch and push remotes. SSH and HTTPS addresses for the same GitHub or Bitbucket Cloud repository resolve to the same remote. The built-in SSH endpoints `ssh.github.com:443` and `altssh.bitbucket.org:443` resolve to their public hosts. Bitbucket Data Center's default `ssh://git@host:7999/PROJECT/repo` resolves to `https://host/scm/PROJECT/repo`, following [Atlassian's clone URL examples](https://confluence.atlassian.com/bitbucketserver094/setting-up-ssh-port-forwarding-1489802866.html). Local checkout bindings keep an existing repository ID stable when its remote changes.

Git `url.*.insteadOf` rewrites often send traffic through a local credential proxy. Convos reads the configured remote when the effective URL is loopback, so the proxy token does not become repository identity. A remote configured *directly* as a loopback URL contributes no remote evidence unless an explicit global mapping resolves it.

Custom mappings live in `~/.convos/config.json` (or `$CONVOS_PROJECT_ROOT/config.json`). They apply to every project on this machine:

```json
{
  "git_identity": {
    "host_aliases": {
      "git.example.com": "code.example.com"
    },
    "url_prefixes": {
      "https://git.example.com:7999/": "https://code.example.com/scm/",
      "https://127.0.0.1:9000/session-token/": "https://code.example.com/"
    }
  }
}
```

`host_aliases` changes just the host. `url_prefixes` changes a normalized URL prefix, including path components; it takes precedence over the built-in host and Bitbucket port rules. Prefixes must start with `https://` and end with `/`; the longest matching prefix wins. Use a prefix mapping for a custom Bitbucket SSH port, host, or web context path. Keep session credentials out of remote URLs when possible. The config is local and is not shared with teammates.

Schema 18 backs up the archive before reconciling older repository rows whose stored remote is a loopback proxy. It uses a unique same-lineage, longest-path candidate from stored public remotes and verified live checkouts. Ambiguous rows remain untouched. Rekeying updates files, versions, checkpoints, edit links, scopes, and signed fact associations; retained signed bodies remain exportable and replayable. The backup verification hashes are separate from the rekey execution time.
