# Francis Docs

This folder contains the documentation for Francis, which is rendered with Hugo and deployed at [`https://gofrancis.dev`](https://gofrancis.dev). The docs are served and built by Vercel.

## Helm chart repository

The site also serves the Helm chart repository at [`https://gofrancis.dev/charts`](https://gofrancis.dev/charts/index.yaml).

The release workflow pushes every chart as an OCI artifact to `ghcr.io/italypaleale/charts/francis`, and `build.sh` runs [`cmd/helm-index`](cmd/helm-index) to generate `static/charts/index.yaml` from the charts in that registry. Each entry points at the chart's `oci://` reference, so Helm downloads charts straight from the registry and the site only serves the index. The build fails if the registry can't be read, so a broken lookup never replaces a working index.

Because the index is generated at build time, the docs must be redeployed after a release for the new chart to appear. The release workflow does that by calling a Vercel deploy hook stored in the `VERCEL_DEPLOY_HOOK_URL` repository secret. To set it up, create a deploy hook for the `main` branch in the Vercel project's **Settings → Git → Deploy Hooks**, then save its URL as an Actions secret named `VERCEL_DEPLOY_HOOK_URL`. Without the secret, new charts are listed the next time the docs are deployed.
