# Publishing and secrets

1. Enable npm trusted publishing for this repository, or create an npm automation token with publish access to `@mohamedhabibwork/queuekit`.
2. In GitHub: **Settings → Secrets and variables → Actions**, add repository secret `NPM_TOKEN`.
3. Create a GitHub Release tagged `v0.1.0` (the tag must equal `package.json`'s version).
4. The release workflow runs the full quality gate and invokes `npm publish --provenance`.

`.env` is intentionally ignored. A file on a developer machine is not available to GitHub-hosted runners, so it cannot securely populate Actions. For local publishing, copy `.env.example` to `.env`, export `NPM_TOKEN`, and run `npm publish`; CI uses `${{ secrets.NPM_TOKEN }}` instead.
