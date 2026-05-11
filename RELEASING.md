# Releasing `@arbitro/client`

This document describes how versions are cut and published to npm. It complements `CONTRIBUTING.md`, which covers day-to-day PRs.

## Versioning policy — SemVer

We follow [Semantic Versioning 2.0](https://semver.org/) with the standard pre-1.0 interpretation:

| Bump | When |
|---|---|
| `0.x.0 → 0.(x+1).0` | Any breaking change in the public API (wire protocol bump, removed export, signature change). Pre-1.0 SemVer allows breaking on minor. |
| `0.x.y → 0.x.(y+1)` | Backwards-compatible feature, bug fix, perf improvement, internal refactor. |
| `0.x.* → 1.0.0` | Stability commitment. Wire protocol locked, public API frozen except via deprecation. Don't bump to 1.0 lightly. |

The version in `package.json` is the source of truth. Git tags mirror it.

## Alignment with the broker

The broker (`arbitro-server`) and this client share the wire protocol but are versioned independently. Today both are at `0.1.0`. They MAY drift — a client patch (e.g. `0.1.1`) is fine without a broker release, and vice versa.

If a client release REQUIRES a specific broker version (e.g. uses an opcode the broker just added), document it in the release notes:

> `@arbitro/client@0.2.0` requires `arbitro-server >= 0.2.0` (uses Action 0x0506 ConsumerSubjects).

## Pre-release checklist

Before cutting a release:

1. **Master is green.** All required CI checks pass on the latest commit.
2. **Integration tests pass against the broker version this client will pair with.**
   ```bash
   ARBITRO_IMAGE=ghcr.io/arbitro-io/arbitro-server:<target-tag> npm run test:integration
   ```
3. **CHANGELOG is updated.** Add a section for the new version with `Added` / `Changed` / `Fixed` / `Removed` / `Deprecated` headers (Keep a Changelog format).
4. **No `WIP` or `TODO release` comments** in the diff since the previous tag.
5. **`peerDependencies` ranges still make sense** for the supported Node/dep versions.

## Cutting the release

```bash
# 1. Bump the version in package.json AND create a matching git tag.
npm version <patch|minor|major>      # writes package.json, creates tag vX.Y.Z

# 2. Push commit and tag.
git push origin master
git push origin vX.Y.Z

# 3. Publish to npm. The package has publishConfig.access=public so the
#    --access public flag is implicit, but pass it for clarity in logs.
npm publish --access public
```

`npm publish` runs the `prepublishOnly` script first, which does typecheck + build. If either fails, nothing is published.

## After publishing

1. **Create a GitHub Release** from the tag with the changelog entry as the body. Optional but recommended — gives downstream users a single page to find release notes.
2. **Verify the package on npm** at https://www.npmjs.com/package/@arbitro/client. Tarball preview should match expectations.
3. **Smoke install** in a clean directory:
   ```bash
   mkdir /tmp/arbitro-smoke && cd /tmp/arbitro-smoke
   npm init -y
   npm install @arbitro/client
   node -e "const {ArbitroClient} = require('@arbitro/client'); console.log(ArbitroClient)"
   ```

## If you need to yank a release

npm allows `npm deprecate` to mark a version as broken without unpublishing (which is irreversible after 72h and only allowed in narrow conditions). Prefer deprecate:

```bash
npm deprecate @arbitro/client@0.1.3 "Critical bug in publish path; upgrade to 0.1.4"
```

Then ship a fix as `0.1.4` immediately.

## Pre-releases

For testing breaking changes before a final release, use SemVer pre-release identifiers:

```bash
npm version preminor --preid=beta    # 0.1.5 → 0.2.0-beta.0
npm publish --tag beta               # publishes under :beta dist-tag, NOT :latest
```

Users opt in with `npm install @arbitro/client@beta`. The `:latest` dist-tag is reserved for stable releases only.

## Maintainer access

Only org members with `npm` publish rights for the `@arbitro` scope can run `npm publish`. Today that's `zenozaga`. To add a maintainer, ask the scope owner to run:

```bash
npm access grant read-write @arbitro:developers @arbitro/client
```

(or use the npm web UI under Teams / Access).
