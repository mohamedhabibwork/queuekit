# Changelog

## 1.0.0 (2026-09-04)


### Features

* **ci:** add multi-package npm publish pipeline ([39b5f2d](https://github.com/mohamedhabibwork/queuekit/commit/39b5f2d91fa92ce262266880eb598d68008f116d))
* **ci:** add release-please for automated changelog + version bumps ([07f46e2](https://github.com/mohamedhabibwork/queuekit/commit/07f46e2f2f139bc83201a55693fb07606529bc91))
* **core:** add tryQueueKitError helper for narrowing Queue Kit errors ([0e7d65c](https://github.com/mohamedhabibwork/queuekit/commit/0e7d65c457189cf5c8eac3e57f2c2c1daa859380))


### Bug Fixes

* **ci:** add checkout to publish detect-package job ([66c868d](https://github.com/mohamedhabibwork/queuekit/commit/66c868d7e4bae6193c244f03fd17b0a1feca0ea7))
* **ci:** add missing outputs mapping on parse-tag job ([b282fd0](https://github.com/mohamedhabibwork/queuekit/commit/b282fd02c66b6c67e9748a452da65f4f2f020619))
* **ci:** drop --provenance so workflow runs without OIDC trusted publisher setup ([6bb50b1](https://github.com/mohamedhabibwork/queuekit/commit/6bb50b1ffb0731f17eff554b3a5a3357a79a612f))
* **ci:** fix indentation on pre-flight step (was 2 spaces, needed 6) ([0dc0381](https://github.com/mohamedhabibwork/queuekit/commit/0dc03814d7bcea07f6b9c923b615e67e096ccaf1))
* **ci:** inline tag-parse in each job, no cross-job outputs ([1146442](https://github.com/mohamedhabibwork/queuekit/commit/11464424f9f55be50d091451cda59b5088cb6f82))
* **ci:** quote step name with colon (YAML scanner error) ([2da7693](https://github.com/mohamedhabibwork/queuekit/commit/2da76939a7aa785667a4b1a4a161bdbca498bb17))
* **ci:** remove @ from publish job name to avoid trigger-quoting bug ([bf85b2d](https://github.com/mohamedhabibwork/queuekit/commit/bf85b2d738c889f2f32ab3c2a8fa9b36e4234818))
* **ci:** remove pre-flight scope check (blocks personal-scope publish) ([a82bb40](https://github.com/mohamedhabibwork/queuekit/commit/a82bb4096794e2aa1e7ffc4a5ebbf71ae95d81ed))
* **ci:** rename detect-package to parse-tag, persist outputs explicitly ([ae6be2b](https://github.com/mohamedhabibwork/queuekit/commit/ae6be2bbfd679605809563fec923c30457d5810d))
* **ci:** support optional RELEASE_PLEASE_TOKEN PAT in release-please ([0504ef7](https://github.com/mohamedhabibwork/queuekit/commit/0504ef7dab12a4b64c604eabea3f0d4aa92fd16b))
* **core,memory:** emit .d.ts via tsc and silence TS 7 baseUrl deprecation ([14e6999](https://github.com/mohamedhabibwork/queuekit/commit/14e69999d92bb62cc761609b37b47505ac61e98d))
* **deps:** pin root typescript to ^6.0.3 for typescript-eslint ([a2f6bb9](https://github.com/mohamedhabibwork/queuekit/commit/a2f6bb9eba2c4b6d5a260faedc1ec5a4eeb3a2ae))
* **deps:** regenerate pnpm-lock.yaml to match package.json ([d0afcf0](https://github.com/mohamedhabibwork/queuekit/commit/d0afcf0186de6ad818ff1e5c4724644d63eaa1cf))
