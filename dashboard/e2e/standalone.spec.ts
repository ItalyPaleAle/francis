import { EMBEDDED_URL, MANAGEMENT_TOKEN, STANDALONE_PORT, STANDALONE_URL } from './cluster.mjs'
import { ENDPOINTS_KEY, expect, heading, test } from './helpers'

const endpointHost = new URL(EMBEDDED_URL).hostname
const endpointPort = new URL(EMBEDDED_URL).port

test('the standalone dashboard connects to an endpoint across origins', async ({ page }) => {
    await page.goto(STANDALONE_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(page.getByText(STANDALONE_URL, { exact: true })).toBeVisible()

    // Adding an endpoint saves it and connects to it
    await page.getByLabel('Host or IP address').fill(endpointHost)
    await page.getByLabel('Port').fill(endpointPort)
    await page.getByLabel('Name (optional)').fill('E2E cluster')
    await page.getByRole('button', { name: 'Add and connect' }).click()

    await expect(page.getByRole('heading', { name: 'E2E cluster' })).toBeVisible()
    await page.getByLabel('API token').fill(MANAGEMENT_TOKEN)
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(heading(page)).toHaveText('Overview')
    await expect(page.getByText(/^Remote topology/)).toBeVisible()

    // Requests go straight to the endpoint
    const request = page.waitForRequest((req) => req.url().startsWith(`${EMBEDDED_URL}/api/v1/hosts`))
    await page.getByRole('navigation', { name: 'Main' }).getByRole('link', { name: 'Hosts', exact: true }).click()
    await request
    await expect(page.getByText('connected', { exact: true }).first()).toBeVisible()

    // Actor state is read across origins too
    await page.goto(`${STANDALONE_URL}/actors/counter/user-01`)
    await page.getByRole('button', { name: 'Read state' }).click()
    await expect(page.getByText('18446744073709551615')).toBeVisible()

    // The endpoint is saved in local storage, and the tab stays connected across reloads
    const saved = await page.evaluate((key) => JSON.parse(localStorage.getItem(key) ?? '[]'), ENDPOINTS_KEY)
    expect(saved).toEqual([{ url: EMBEDDED_URL, name: 'E2E cluster' }])
    await page.reload()
    await expect(heading(page)).toHaveText('user-01')
    await expect(page.getByText('E2E cluster')).toBeVisible()

    // Switching goes back to the list, where the endpoint is marked as signed in
    await page.getByRole('button', { name: 'Switch' }).click()
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    const item = page.getByRole('listitem').filter({ hasText: 'E2E cluster' })
    await expect(item.getByText('Signed in')).toBeVisible()

    // Connecting again needs no new sign-in
    await item.getByRole('button', { name: 'Connect' }).click()
    await expect(heading(page)).toHaveText('user-01')

    await page.getByRole('button', { name: 'Switch' }).click()
    await item.getByRole('button', { name: 'Remove' }).click()
    await expect(page.getByRole('listitem')).toHaveCount(0)
    expect(await page.evaluate((key) => localStorage.getItem(key), ENDPOINTS_KEY)).toBe('[]')
})

test('the endpoint form rejects an invalid address', async ({ page }) => {
    await page.goto(STANDALONE_URL)
    await page.getByLabel('Host or IP address').fill('not a host')
    await page.getByRole('button', { name: 'Add and connect' }).click()
    await expect(page.getByRole('alert')).toHaveText('Enter a host name or IP address, and a port between 1 and 65535.')
})

test.describe('from an origin the endpoint does not allow', () => {
    // The browser reports the refused preflight, in its own words: Chromium first, then WebKit
    test.use({
        allowedConsoleErrors: /CORS policy|net::ERR_FAILED|Preflight response is not successful|access control checks/,
    })

    test('signing in explains the missing allowed origin', async ({ page }) => {
        const origin = `http://localhost:${STANDALONE_PORT}`
        await page.goto(origin)
        await page.getByLabel('Host or IP address').fill(`${endpointHost}:${endpointPort}`)
        await page.getByRole('button', { name: 'Add and connect' }).click()

        await page.getByLabel('API token').fill(MANAGEMENT_TOKEN)
        await page.getByRole('button', { name: 'Sign in' }).click()
        await expect(page.getByRole('alert')).toHaveText(
            `Couldn't reach ${EMBEDDED_URL}. Check that it's running, and that its management.allowedOrigins setting includes ${origin}.`
        )
    })
})

test('the standalone page may call other origins, and the embedded one only its own', async ({ request }) => {
    const standalone = await request.get(STANDALONE_URL)
    expect(standalone.headers()['content-security-policy']).toContain("connect-src 'self' http: https:;")
    expect(await standalone.text()).toContain('<meta name="francis-dashboard-mode" content="standalone"')

    const embedded = await request.get(`${EMBEDDED_URL}/`)
    expect(embedded.headers()['content-security-policy']).toContain("connect-src 'self';")
    expect(await embedded.text()).toContain('<meta name="francis-dashboard-mode" content="embedded"')
})
