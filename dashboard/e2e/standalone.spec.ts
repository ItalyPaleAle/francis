import { EMBEDDED_URL, MANAGEMENT_TOKEN, STANDALONE_PORT, STANDALONE_URL } from './cluster.mjs'
import { ENDPOINTS_KEY, expect, freePort, heading, saveEndpoints, test } from './helpers'

test('the standalone dashboard connects to an endpoint across origins', async ({ page }) => {
    // With nothing saved, the page opens on the form
    await page.goto(STANDALONE_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Saved endpoints' })).toBeHidden()

    // The info button next to the address names the origin to allow
    await expect(page.getByText(STANDALONE_URL, { exact: true })).toBeHidden()
    await page.getByRole('button', { name: 'About allowed origins' }).click()
    await expect(page.getByText(STANDALONE_URL, { exact: true })).toBeVisible()
    await page.keyboard.press('Escape')
    await expect(page.getByText(STANDALONE_URL, { exact: true })).toBeHidden()

    // Adding an endpoint checks it, saves it, and connects to it
    const check = page.waitForRequest(`${EMBEDDED_URL}/healthz`)
    await page.getByLabel('URL').fill(EMBEDDED_URL)
    await page.getByLabel('Name (optional)').fill('E2E cluster')
    await page.getByRole('button', { name: 'Add and connect' }).click()
    await check

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

    // The sidebar names the endpoint under Sign out, with its full address on hover
    const connected = page.getByTitle(EMBEDDED_URL, { exact: true })
    await expect(connected).toContainText('E2E cluster')
    await expect(connected).toContainText(EMBEDDED_URL)

    // Signing out goes back to the list, and the token goes with the session, so connecting again asks for one
    await page.getByRole('button', { name: 'Sign out' }).click()
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    const item = page.getByRole('listitem').filter({ hasText: 'E2E cluster' })
    await item.getByRole('button', { name: 'E2E cluster' }).click()
    await expect(page.getByRole('heading', { name: 'E2E cluster' })).toBeVisible()
    await page.getByLabel('API token').fill(MANAGEMENT_TOKEN)
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(heading(page)).toHaveText('user-01')

    // The form opens from the list, and going back leaves the list as it was
    await page.getByRole('button', { name: 'Sign out' }).click()
    await page.getByRole('button', { name: 'Add an endpoint' }).click()
    await expect(page.getByRole('heading', { name: 'Add an endpoint' })).toBeVisible()
    await expect(page.getByLabel('URL')).toBeFocused()
    await page.getByRole('button', { name: 'Saved endpoints' }).click()
    await expect(page.getByRole('listitem')).toHaveCount(1)

    // Removing the last endpoint opens the form
    await item.getByRole('button', { name: 'Remove' }).click()
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(page.getByLabel('URL')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Saved endpoints' })).toBeHidden()
    expect(await page.evaluate((key) => localStorage.getItem(key), ENDPOINTS_KEY)).toBe('[]')
})

test('a saved endpoint shows its remove button beside the list while the pointer is on it', async ({ page }) => {
    await saveEndpoints(page, [
        { url: EMBEDDED_URL, name: 'E2E cluster' },
        { url: 'http://127.0.0.1:7401', name: 'Elsewhere' },
    ])
    await page.goto(STANDALONE_URL)

    // The button fades in rather than appearing, so its holder's opacity tells whether it shows
    const list = page.getByRole('list')
    const item = list.getByRole('listitem').filter({ hasText: 'E2E cluster' })
    const remove = item.getByRole('button', { name: 'Remove' })
    const holder = remove.locator('..')
    await expect(holder).toHaveCSS('opacity', '0')
    await item.getByRole('button', { name: 'E2E cluster' }).hover()
    await expect(holder).toHaveCSS('opacity', '1')

    // It sits outside the list, to the right of its row
    const listBox = await list.boundingBox()
    const removeBox = await remove.boundingBox()
    expect(removeBox?.x).toBeGreaterThanOrEqual((listBox?.x ?? 0) + (listBox?.width ?? 0))

    // Removing one endpoint leaves the others
    await remove.click()
    await expect(list.getByRole('listitem')).toHaveCount(1)
    await expect(list.getByRole('button', { name: 'Elsewhere' })).toBeVisible()
})

test('the endpoint form rejects an invalid address', async ({ page }) => {
    await page.goto(STANDALONE_URL)
    await page.getByLabel('URL').fill('not a host')
    await page.getByRole('button', { name: 'Add and connect' }).click()
    await expect(page.getByRole('alert')).toHaveText('Enter a URL such as https://1.2.3.4:7401.')
})

test('the endpoint form refuses a server that is not a management API', async ({ page }) => {
    // The dashboard's own server answers, but not as a management API
    await page.goto(STANDALONE_URL)
    await page.getByLabel('URL').fill(STANDALONE_URL)
    await page.getByRole('button', { name: 'Add and connect' }).click()
    await expect(page.getByRole('alert')).toHaveText(
        `${STANDALONE_URL} responded, but it isn't a Francis management API. Check the address and the port.`
    )
    expect(await page.evaluate((key) => localStorage.getItem(key), ENDPOINTS_KEY)).toBeNull()
})

test.describe('an endpoint that is not running', () => {
    // The browser reports the refused connection, in its own words: Chromium first, then WebKit
    test.use({ allowedConsoleErrors: /ERR_CONNECTION_REFUSED|Could not connect to the server/ })

    test('adding it explains that it could not be reached', async ({ page }) => {
        const url = `http://127.0.0.1:${await freePort()}`
        await page.goto(STANDALONE_URL)
        await page.getByLabel('URL').fill(url)
        await page.getByRole('button', { name: 'Add and connect' }).click()
        await expect(page.getByRole('alert')).toHaveText(
            `Couldn't reach ${url}. Check the address, and that the runtime is running.`
        )
        expect(await page.evaluate((key) => localStorage.getItem(key), ENDPOINTS_KEY)).toBeNull()
    })
})

test.describe('from an origin the endpoint does not allow', () => {
    // The browser reports the refused request, in its own words: Chromium first, then WebKit
    test.use({
        allowedConsoleErrors:
            /CORS policy|net::ERR_FAILED|Preflight response is not successful|access control checks|not allowed by Access-Control-Allow-Origin/,
    })

    test('adding the endpoint names the origin to allow', async ({ page }) => {
        const origin = `http://localhost:${STANDALONE_PORT}`
        await page.goto(origin)
        // An address without a scheme gets the page's own
        await page.getByLabel('URL').fill(new URL(EMBEDDED_URL).host)
        await page.getByRole('button', { name: 'Add and connect' }).click()
        await expect(page.getByRole('alert')).toHaveText(
            `${EMBEDDED_URL} doesn't allow requests from this page. Add ${origin} to its management.allowedOrigins setting.`
        )
        expect(await page.evaluate((key) => localStorage.getItem(key), ENDPOINTS_KEY)).toBeNull()
    })

    test('signing in to a saved endpoint names the origin to allow', async ({ page }) => {
        // The endpoint was saved before it stopped allowing the origin, so it skips the check
        const origin = `http://localhost:${STANDALONE_PORT}`
        await saveEndpoints(page, [{ url: EMBEDDED_URL, name: '' }])
        await page.goto(origin)
        await page.getByRole('button', { name: new URL(EMBEDDED_URL).host }).click()

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
