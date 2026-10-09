import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

// Addresses and credentials of the cluster the Playwright tests run against, shared by the setup script and the tests
// The ports are fixed and unusual, so a local runtime on the default ports doesn't get in the way

export const RUNTIME_ADDRESS = '127.0.0.1:17400'
export const EMBEDDED_URL = 'http://127.0.0.1:17401'
export const STANDALONE_PORT = 17402
export const STANDALONE_URL = `http://127.0.0.1:${STANDALONE_PORT}`
export const CONTROL_URL = 'http://127.0.0.1:17499'
export const SEED_HOST_ADDRESS = '127.0.0.1:17571'

export const READ_ONLY_TOKEN = 'e2e-readonly-token-0123456789abcdefghijkl'
export const MANAGEMENT_TOKEN = 'e2e-management-token-0123456789abcdefghij'
export const RUNTIME_PSK = 'e2e-runtime-psk-0123456789abcdef'
// Must match hostBootstrapPSK in e2e/fixture/main.go
export const HOST_BOOTSTRAP_PSK = 'e2e-host-bootstrap-psk-0123456789abcdef'

// The binaries "pnpm run e2e:build" builds
export const BIN_DIR = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '.bin', 'e2e')
export const RUNTIME_BIN = resolve(BIN_DIR, 'francis')
export const FIXTURE_BIN = resolve(BIN_DIR, 'fixture-host')
