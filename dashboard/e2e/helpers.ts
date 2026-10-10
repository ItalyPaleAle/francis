import { type ChildProcess, spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:net'
import { test as base, expect, type Page } from '@playwright/test'

import {
    CONTROL_URL,
    EMBEDDED_URL,
    FIXTURE_BIN,
    MANAGEMENT_TOKEN,
    RUNTIME_ADDRESS,
    SEED_HOST_ADDRESS,
} from './cluster.mjs'

export { expect }

// The dashboard's session storage keys
export const TOKEN_KEY = 'francis-dashboard-token'
export const ENDPOINTS_KEY = 'francis-dashboard-endpoints'

interface Fixtures {
    // Errors the test expects the browser to report, such as a request CORS refused
    // It's a single pattern, since Playwright reads an array passed to test.use as a value and its options
    allowedConsoleErrors: RegExp | null
}

// test fails a test whose page threw, or logged an error such as a Content-Security-Policy violation
export const test = base.extend<Fixtures>({
    allowedConsoleErrors: [null, { option: true }],
    page: async ({ page, allowedConsoleErrors }, use) => {
        const problems: string[] = []
        page.on('pageerror', (err) => {
            // WebKit reports a request refused by CORS as an error of the page, even when the code handles it
            if (!allowedConsoleErrors?.test(err.message)) {
                problems.push(`uncaught: ${err.message}`)
            }
        })

        page.on('console', (msg) => {
            if (msg.type() !== 'error') {
                return
            }

            // Several pages expect error statuses from the API, and the browser logs each of them
            const text = msg.text()
            if (/Failed to load resource: the server responded with a status of/.test(text)) {
                return
            }
            if (allowedConsoleErrors?.test(text)) {
                return
            }
            problems.push(`console: ${text}`)
        })

        await use(page)

        expect(problems, 'the page reported errors').toEqual([])
    },
})

// signIn signs in through the form of the dashboard served next to the API
export async function signIn(page: Page, token = MANAGEMENT_TOKEN) {
    await page.goto('/')
    await page.getByLabel('API token').fill(token)
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(page.getByRole('heading', { level: 1, name: 'Overview' })).toBeVisible()
}

// useToken signs the page in before it loads, skipping the form for tests about other things
export async function useToken(page: Page, token = MANAGEMENT_TOKEN) {
    await page.addInitScript(
        ([key, value]) => {
            if (!sessionStorage.getItem(key)) {
                sessionStorage.setItem(key, value)
            }
        },
        [TOKEN_KEY, token]
    )
}

// saveEndpoints fills the standalone dashboard's list of endpoints before the page loads, skipping the form for tests about other things
export async function saveEndpoints(page: Page, list: { url: string; name: string }[]) {
    await page.addInitScript(
        ([key, value]) => {
            if (!localStorage.getItem(key)) {
                localStorage.setItem(key, value)
            }
        },
        [ENDPOINTS_KEY, JSON.stringify(list)]
    )
}

// heading returns the page's main heading
// filterButton is the trigger of a multi-select filter, whose name is the filter's label followed by its value
export function filterButton(page: Page, label: string) {
    return page.getByRole('button', { name: new RegExp(`^${label} `) })
}

// filterOption is a value in an open multi-select filter, which a click checks or unchecks
export function filterOption(page: Page, label: string, value: string) {
    return page.getByRole('group', { name: label }).getByText(value, { exact: true })
}

// pickOnly narrows a multi-select filter to one value, which applies at once, then closes the filter
export async function pickOnly(page: Page, label: string, value: string) {
    await filterButton(page, label).click()
    await page
        .getByRole('group', { name: label })
        .getByRole('button', { name: `Only ${value}`, exact: true })
        .click()
    await page.keyboard.press('Escape')
}

// chooseOne picks the value of a filter that takes a single value, which applies at once and closes the filter
export async function chooseOne(page: Page, label: string, value: string) {
    await filterButton(page, label).click()
    await filterOption(page, label, value).click()
}

// utcTime matches the exact time in UTC that dates show on hover
export const utcTime = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} UTC$/

export function heading(page: Page) {
    return page.locator('main header h1')
}

// pageStatus returns the status badge next to the page's main heading
export function pageStatus(page: Page) {
    return page.locator('main header span:has(> h1) + span')
}

// refreshUntil clicks the page's Refresh button until the check passes, for state that changes after an action returns
export async function refreshUntil(page: Page, check: () => Promise<void>, timeout = 20_000) {
    await expect(async () => {
        await page.getByRole('button', { name: 'Refresh' }).click()
        await check()
    }).toPass({ timeout, intervals: [500, 1000] })
}

export function uniqueID(prefix: string) {
    return `${prefix}-${randomUUID().slice(0, 8)}`
}

// api calls the management API with the management token
export async function api<T>(path: string): Promise<T> {
    const res = await fetch(`${EMBEDDED_URL}/api/v1${path}`, {
        headers: { Authorization: `Bearer ${MANAGEMENT_TOKEN}` },
    })
    if (!res.ok) {
        throw new Error(`GET ${path} returned ${res.status}: ${await res.text()}`)
    }

    return (await res.json()) as T
}

interface HostItem {
    hostId: string
    address: string
}

// hostID returns the ID of the host registered at an address, waiting for it to register
export async function hostID(address = SEED_HOST_ADDRESS): Promise<string> {
    let id = ''
    await expect(async () => {
        const page = await api<{ items: HostItem[] }>('/hosts')
        const host = page.items.find((h) => h.address === address)
        expect(host, `no host is registered at ${address}`).toBeDefined()
        id = host?.hostId ?? ''
    }).toPass({ timeout: 30_000 })
    return id
}

// control calls the fixture host's control API, which creates the actors and instances a test changes
export async function control(path: string) {
    const res = await fetch(`${CONTROL_URL}${path}`, { method: 'POST' })
    if (!res.ok) {
        throw new Error(`POST ${path} returned ${res.status}: ${await res.text()}`)
    }
}

// activateActor activates a fresh counter actor, which also stores its state
export async function activateActor(id: string) {
    await control(`/actors/${encodeURIComponent(id)}`)
}

// startInstance starts a checkout instance, returning once it waits for its approval
export async function startInstance(id: string) {
    await control(`/instances/${encodeURIComponent(id)}`)
}

// freePort returns a port that nothing listened on a moment ago
export async function freePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const srv = createServer()
        srv.once('error', reject)
        srv.listen(0, '127.0.0.1', () => {
            const addr = srv.address()
            srv.close(() => resolve(typeof addr === 'object' && addr ? addr.port : 0))
        })
    })
}

export interface DrainableHost {
    hostId: string
    actorType: string
    process: ChildProcess
    exited: Promise<number | null>
}

// startDrainableHost runs a host that is the only server of its own actor type, so draining it affects nothing else
export async function startDrainableHost(): Promise<DrainableHost> {
    const actorType = uniqueID('drainable')
    const address = `127.0.0.1:${await freePort()}`
    const child = spawn(
        FIXTURE_BIN,
        ['-mode', 'drainable', '-address', address, '-runtime-address', RUNTIME_ADDRESS, '-actor-type', actorType],
        { stdio: 'ignore' }
    )
    const exited = new Promise<number | null>((resolve) => child.on('exit', (code) => resolve(code)))

    return { hostId: await hostID(address), actorType, process: child, exited }
}
