# Runbook: rotating the Fly deploy token

The CD job (`deploy` in `.github/workflows/ci.yml`) authenticates to Fly with
`FLY_API_TOKEN`, a **deploy-scoped** token stored only as a secret of the
GitHub environment `production`. The token is created with a 720 h (30 day)
expiry on purpose: a perpetual token was revoked on 2026-08-27 because in a
CD pipeline a leaked deploy token is a permanent write to production
(`fly deploy` runs whatever image it is handed). A token that expires
bounds that damage to one month, at the price of this runbook.

Nothing user-facing depends on the token. If it expires, production keeps
serving; only deploys stop (the job fails at `flyctl` authentication).

## Roles

Two identities, as everywhere in this repository:

- **The account owner** (a human: owns the Fly app, is admin of the GitHub
  repository through the browser) creates the token, installs the secret,
  approves the proving deploy and revokes the old token. The token value
  passes through their hands only.
- **The automation identity** (the machine account used by CLI tooling and
  by any AI-agent session) may run the read-only checks, watch the deploy
  run and update this file. It never runs a command that prints a
  credential into a terminal transcript or a log.

## Schedule

| Token | Created | Expires (UTC) | Rotate by |
|---|---|---|---|
| `BgRyRGOe…` (current) | 2026-08-27 | 2026-09-26 17:02 | **2026-09-24** |
| next | day of rotation | +720 h | expiry − 2 days |

Rotate two days before expiry so a failed attempt still has a working token
behind it. This table is the record: step 6 updates it in the same act as
the rotation.

## Procedure

### 1. Pre-flight (read-only; either role)

```bash
fly auth whoami                          # expected: the account that owns the app
fly tokens list -a llm-gateway-v1        # note the ID and EXPIRES AT of the live token
```

Expected: exactly one unrevoked `flyctl deploy token` row. Two unrevoked
rows mean a previous rotation did not finish step 5.

### 2. Create the new token — output to a file, never to a transcript

A command that **prints** a credential runs with its output redirected. Do
this in a plain terminal, not inside any session that logs commands.

```bash
umask 077
mkdir -p ~/.config/fly
fly tokens create deploy -a llm-gateway-v1 -n "deploy $(date +%F)" -x 720h \
  > ~/.config/fly/deploy-token-$(date +%F).txt
ls -la ~/.config/fly/                    # file exists, mode 600
```

`-x 720h` keeps the 30-day life decided on 2026-08-27. Do not `cat` the file
where the output is logged; open it in an editor or use it by indirection.

### 3. Install the secret in the GitHub environment (account owner, browser)

Repository → **Settings → Environments → production → Environment secrets →
`FLY_API_TOKEN` → Update** → paste the file's single line.

It must be the **environment** secret, not a repository secret: the job
reads `secrets.FLY_API_TOKEN` under `environment: production`, and a
repository secret of the same name would be reachable by any workflow edit
without the human gate. (The repository-level copy was deleted on 2026-08-27.)

CLI alternative, only under the account owner's own GitHub login, never the
automation identity (it has no admin rights and must stay that way):

```bash
gh secret set FLY_API_TOKEN --env production --repo pedroives-public/llm-gateway \
  < ~/.config/fly/deploy-token-$(date +%F).txt
```

Afterwards log the CLI back out of the owner's identity so it carries only
the automation identity (`gh auth status` shows a single account).

### 4. Prove the new token with a real deploy

The proof is a green `deploy` job that used the new secret, not a token
listing. Either merge the next pending PR to `main`, or in **Actions → last
successful `ci` run → Re-run jobs → deploy** (a re-run reads secrets at run
time). Approve the deployment in the browser when the job waits on the
`production` gate.

Expected: `flyctl deploy --remote-only` succeeds and the three live smokes
pass (`/health` 200, the 401 envelope, one authenticated `max_tokens: 1`
request). Reading logs is not a gate. A failure at the `flyctl` step with a
401/403 means the secret has a stray newline or space, was pasted into the
wrong environment, or the token was created for another app — fix the
secret and re-run; the old token is still valid at this point, nothing is
down.

### 5. Revoke the old token — only after step 4 is green

```bash
fly tokens revoke <OLD_ID>               # the ID noted in step 1
fly tokens list -a llm-gateway-v1        # the old row now shows REVOKED AT; the new row is the only live one
```

Removal is proven by the listing, never by the exit code of `revoke`.

### 6. Record, in the same act

- Update the Schedule table above: new token prefix, creation date, expiry,
  "rotate by" (expiry − 2 days). Commit it with the rotation.
- Delete the local token file and prove it by listing:

```bash
rm ~/.config/fly/deploy-token-*.txt && ls ~/.config/fly/
```

## If the token leaked

A token that reached a transcript, a log, a chat or a screenshot is
compromised even if nobody used it: revoke it (step 5) **first**, then
create and install a new one (steps 2–4). Order matters; the pipeline can
wait, the exposure cannot.

## Why not a longer expiry

Fly's own CLI text recommends a shorter expiry when practical. 720 h was
chosen because the human step (browser paste + approve) is ~5 minutes a
month, and the alternative — a token that never expires — was the exact
condition measured on 2026-08-26: repository write → workflow edit → token
printed → arbitrary deploy → every secret readable from inside. A monthly
rotation is the cheapest structural bound on that chain.
