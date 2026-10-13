import type { Page } from '@playwright/test'

import { EMBEDDED_URL, HOSTED_URL, MANAGEMENT_TOKEN, STANDALONE_URL } from './cluster.mjs'
import { expect, heading, test } from './helpers'

function hostedNotice(page: Page) {
    return page.getByRole('contentinfo').getByRole('region', { name: 'About my.gofrancis.dev' })
}

test("the hosted dashboard says what it does with its users' data until they sign in", async ({ page }) => {
    // The footer of the endpoint picker explains that the dashboard keeps nothing, and that it's offered as best-effort
    await page.goto(HOSTED_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(hostedNotice(page)).toContainText("doesn't log or store any of your data")
    await expect(hostedNotice(page)).toContainText('saved endpoints stay only in its local storage')
    await expect(hostedNotice(page)).toContainText('free to use and offered on a best-effort basis')

    // The hosted dashboard is standalone, so it connects to the endpoint across origins
    await page.getByLabel('URL').fill(EMBEDDED_URL)
    await page.getByLabel('Name (optional)').fill('E2E cluster')
    await page.getByRole('button', { name: 'Add and connect' }).click()

    // The sign-in form has it too
    await expect(page.getByRole('heading', { name: 'E2E cluster' })).toBeVisible()
    await expect(hostedNotice(page)).toBeVisible()

    // Signed in, the notice is gone, and the footer only says what powers the dashboard
    await page.getByLabel('API token').fill(MANAGEMENT_TOKEN)
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(heading(page)).toHaveText('Overview')
    await expect(hostedNotice(page)).toHaveCount(0)
    await expect(page.getByRole('contentinfo')).toContainText('Powered by')
})

test('the hosted dashboard installs its service worker', async ({ page }) => {
    await page.goto(HOSTED_URL)
    const scope = await page.evaluate(async () => (await navigator.serviceWorker.ready).scope)
    expect(scope).toBe(`${HOSTED_URL}/`)
})

test('the dashboards the runtime serves have no hosted notice', async ({ page }) => {
    await page.goto(STANDALONE_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(page.getByRole('contentinfo')).toBeVisible()
    await expect(hostedNotice(page)).toHaveCount(0)

    await page.goto(`${EMBEDDED_URL}/`)
    await expect(page.getByRole('heading', { name: 'Management dashboard' })).toBeVisible()
    await expect(page.getByRole('contentinfo')).toBeVisible()
    await expect(hostedNotice(page)).toHaveCount(0)
})
