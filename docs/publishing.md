# Publishing and secrets

1. Enable npm trusted publishing for this repository, or create an npm automation token with publish access to `@mohamedhabibwork/queuekit`.
2. In GitHub: **Settings → Secrets and variables → Actions**, add repository secret `NPM_TOKEN`.
3. Push a tag. Every tag automatically creates or updates its GitHub Release with GitHub-generated release notes, and appends a matching section (notes plus the commits since the previous tag) to `CHANGELOG.md` on `main` as `github-actions[bot]`. Publishing a GitHub Release also regenerates its notes, so the release always has a changelog.
4. Tags beginning with `v` (for example, `v0.1.0`) run the full quality gate and invoke `npm publish --provenance`; the tag must equal `package.json`'s version. Other tags receive release notes and a `CHANGELOG.md` entry but do not publish to npm.

`.env` is intentionally ignored. A file on a developer machine is not available to GitHub-hosted runners, so it cannot securely populate Actions. For local publishing, copy `.env.example` to `.env`, export `NPM_TOKEN`, and run `npm publish`; CI uses `${{ secrets.NPM_TOKEN }}` instead.
