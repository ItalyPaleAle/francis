# Francis Helm repository

This folder builds the Helm chart repository served at [`https://charts.gofrancis.dev`](https://charts.gofrancis.dev/index.yaml), which users add with:

```sh
helm repo add francis https://charts.gofrancis.dev
```

## How it works

The release workflow pushes every chart as an OCI artifact to `ghcr.io/italypaleale/charts/francis`. This folder holds a small Go program that reads every chart version from that registry and writes them into `public/index.yaml`, the only file the site serves.

Each entry in the index points at the chart's `oci://` reference, so Helm downloads charts straight from the registry. Helm 3.9 and newer support this. The index is JSON, which is valid YAML and is also what `helm repo index --json` writes.

The build fails if the registry can't be read or holds no charts, so a broken lookup never replaces a working index.

The root URL redirects to the Helm section of the docs, since Helm never requests it but people will open it in a browser.

## Vercel project

The site is a separate Vercel project from the docs, built by Vercel from this folder. It serves static files only. [`vercel.json`](vercel.json) holds its build settings, so the project needs just:

- **Root Directory**: `helm-repo`
- **Domain**: `charts.gofrancis.dev`
- **Deploy Hook**: one for the `main` branch, in **Settings → Git → Deploy Hooks**, saved as the `HELM_REPO_DEPLOY_HOOK_URL` Actions secret in the GitHub repository

The project deploys to production on every push to `main`. Preview deployments are skipped unless the push changes this folder.

Since the index is built at deploy time, a new chart only appears once the site is redeployed. After pushing a chart, the release workflow calls the deploy hook so that happens right away. Without the secret, new charts appear the next time something is pushed to `main`.

## Running locally

```sh
go run . -out public/index.yaml
```

Then test the result with Helm by serving the `public` folder, for example with `python3 -m http.server -d public 8080` and `helm repo add francis-local http://localhost:8080`.
