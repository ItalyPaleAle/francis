import { spawn } from 'node:child_process'
import { createWriteStream, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import process from 'node:process'
import { createInterface } from 'node:readline'

import {
    BIN_DIR,
    CONTROL_URL,
    EMBEDDED_URL,
    FIXTURE_BIN,
    HOST_BOOTSTRAP_PSK,
    MANAGEMENT_TOKEN,
    READ_ONLY_TOKEN,
    RUNTIME_ADDRESS,
    RUNTIME_BIN,
    RUNTIME_PSK,
    SEED_HOST_ADDRESS,
    STANDALONE_PORT,
    STANDALONE_URL,
} from './cluster.mjs'

// Starts the cluster the Playwright tests run against, and keeps it running until Playwright stops this process
// Playwright waits for the fixture host's /ready endpoint, which answers once the seeded data has settled

for (const bin of [RUNTIME_BIN, FIXTURE_BIN]) {
    if (!existsSync(bin)) {
        throw new Error(`Missing ${bin}. Run "pnpm run e2e:build" first.`)
    }
}

// Every process logs into one file, which stays after the run for debugging
mkdirSync(BIN_DIR, { recursive: true })
const logPath = join(BIN_DIR, 'cluster.log')
const log = createWriteStream(logPath)
process.stderr.write(`E2E cluster logs: ${logPath}\n`)

const tempDir = mkdtempSync(join(tmpdir(), 'francis-dashboard-e2e-'))
const configPath = join(tempDir, 'config.yaml')
const configData = {
    bind: RUNTIME_ADDRESS,
    runtimePSKs: [RUNTIME_PSK],
    bootstrap: {
        method: 'psk',
        hostPSK: HOST_BOOTSTRAP_PSK,
    },
    provider: {
        connectionString: join(tempDir, 'francis.db'),
    },
    management: {
        enabled: true,
        bind: new URL(EMBEDDED_URL).host,
        readOnlyTokens: [READ_ONLY_TOKEN],
        managementTokens: [MANAGEMENT_TOKEN],
        // Only the standalone dashboard's 127.0.0.1 origin is allowed so its localhost origin tests a refused one
        allowedOrigins: [STANDALONE_URL],
    },
    log: {
        level: 'warn',
    },
}

writeFileSync(configPath, JSON.stringify(configData))

const env = {
    ...process.env,
    FRANCIS_CONFIG: configPath,
    OTEL_LOGS_EXPORTER: 'none',
    OTEL_METRICS_EXPORTER: 'none',
    OTEL_TRACES_EXPORTER: 'none',
}

const children = []
let stopping = false

// start runs a process whose output goes to the log, and stops the cluster if it exits on its own
function start(name, args) {
    const child = spawn(args[0], args.slice(1), { env, stdio: ['ignore', 'pipe', 'pipe'] })
    for (const stream of [child.stdout, child.stderr]) {
        createInterface({ input: stream }).on('line', (line) => log.write(`[${name}] ${line}\n`))
    }

    child.on('exit', (code, signal) => {
        if (!stopping) {
            process.stderr.write(`${name} exited unexpectedly (code ${code}, signal ${signal}); see ${logPath}\n`)
            stop(1)
        }
    })
    children.push(child)

    return child
}

function stop(code) {
    if (stopping) {
        return
    }

    stopping = true
    for (const child of children) {
        child.kill('SIGTERM')
    }

    setTimeout(() => {
        rmSync(tempDir, { recursive: true, force: true })
        process.exit(code)
    }, 1000)
}

for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => stop(0))
}

async function waitForHTTP(url, timeoutMs) {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
        try {
            const res = await fetch(url)
            if (res.ok) {
                return
            }
        } catch {
            // Not listening yet
        }
        await new Promise((r) => setTimeout(r, 200))
    }

    throw new Error(`${url} did not answer within ${timeoutMs}ms`)
}

try {
    // The standalone dashboard needs nothing else
    start('dashboard', [RUNTIME_BIN, 'dashboard', '-bind', '127.0.0.1', '-port', String(STANDALONE_PORT)])

    // The runtime comes first, since the host connects to it
    start('runtime', [RUNTIME_BIN])
    await waitForHTTP(`${EMBEDDED_URL}/healthz`, 60_000)

    // The fixture host seeds the data, and its /ready endpoint tells Playwright when it's done
    start('fixture', [
        FIXTURE_BIN,
        '-mode',
        'seed',
        '-address',
        SEED_HOST_ADDRESS,
        '-runtime-address',
        RUNTIME_ADDRESS,
        '-control-address',
        new URL(CONTROL_URL).host,
    ])

    await waitForHTTP(`${STANDALONE_URL}/`, 30_000)
} catch (err) {
    process.stderr.write(`Failed to start the E2E cluster: ${err instanceof Error ? err.message : err}\n`)
    stop(1)
}
