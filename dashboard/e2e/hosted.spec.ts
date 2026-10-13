import type { Page } from '@playwright/test'

import { EMBEDDED_URL, HOSTED_URL, MANAGEMENT_TOKEN, STANDALONE_URL } from './cluster.mjs'
import { expect, heading, test } from './helpers'

function hostedFooter(page: Page) {
    return page.getByRole('contentinfo', { name: 'About the hosted dashboard' })
}

test("the hosted dashboard says what it does with its users' data until they sign in", async ({ page }) => {
    // The endpoint picker explains that the dashboard keeps nothing, and that it's offered as best-effort
    await page.goto(HOSTED_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(hostedFooter(page)).toContainText("doesn't log or store any of your data")
    await expect(hostedFooter(page)).toContainText('saved endpoints stay only in its local storage')
    await expect(hostedFooter(page)).toContainText('free to use and offered on a best-effort basis')

    // It's at the bottom of the window, however short the screen above it
    const box = await hostedFooter(page).boundingBox()
    expect(Math.round((box?.y ?? 0) + (box?.height ?? 0))).toBe(page.viewportSize()?.height)

    // The hosted dashboard is standalone, so it connects to the endpoint across origins
    await page.getByLabel('URL').fill(EMBEDDED_URL)
    await page.getByLabel('Name (optional)').fill('E2E cluster')
    await page.getByRole('button', { name: 'Add and connect' }).click()

    // The sign-in form has it too
    await expect(page.getByRole('heading', { name: 'E2E cluster' })).toBeVisible()
    await expect(hostedFooter(page)).toBeVisible()

    // Signed in, the footer is gone
    await page.getByLabel('API token').fill(MANAGEMENT_TOKEN)
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(heading(page)).toHaveText('Overview')
    await expect(hostedFooter(page)).toHaveCount(0)
})

test('the hosted dashboard installs its service worker', async ({ page }) => {
    await page.goto(HOSTED_URL)
    const scope = await page.evaluate(async () => (await navigator.serviceWorker.ready).scope)
    expect(scope).toBe(`${HOSTED_URL}/`)
})

test('the dashboards the runtime serves have no hosted footer', async ({ page }) => {
    await page.goto(STANDALONE_URL)
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toBeVisible()
    await expect(hostedFooter(page)).toHaveCount(0)

    await page.goto(`${EMBEDDED_URL}/`)
    await expect(page.getByRole('heading', { name: 'Management dashboard' })).toBeVisible()
    await expect(hostedFooter(page)).toHaveCount(0)
})
