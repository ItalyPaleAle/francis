import type { Page } from '@playwright/test'

import { EMBEDDED_URL, HOSTED_URL, STANDALONE_URL } from './cluster.mjs'
import { expect, heading, test, useToken } from './helpers'

// The e2e builds set no version, so the runtime and the hosted build both name the default one
const footerText = 'Powered by Francis · canary'

function footer(page: Page) {
    return page.getByRole('contentinfo')
}

// expectAtBottom checks that the footer ends at the bottom of the window, as it does on a screen shorter than the window
async function expectAtBottom(page: Page) {
    const box = await footer(page).boundingBox()
    expect(Math.round((box?.y ?? 0) + (box?.height ?? 0))).toBe(page.viewportSize()?.height)
}

test('every dashboard links to the Francis website, with its version, before signing in', async ({ page }) => {
    for (const url of [`${EMBEDDED_URL}/`, STANDALONE_URL, HOSTED_URL]) {
        await page.goto(url)
        await expect(footer(page)).toContainText(footerText)
        await expectAtBottom(page)

        // The link opens in a new tab, so following it doesn't leave the dashboard
        const link = footer(page).getByRole('link', { name: 'Francis' })
        await expect(link).toHaveAttribute('href', 'https://gofrancis.dev')
        await expect(link).toHaveAttribute('target', '_blank')
    }
})

test('signed in, every page ends with the footer', async ({ page }) => {
    await useToken(page)
    await page.goto('/')
    await expect(heading(page)).toHaveText('Overview')
    await expect(footer(page)).toContainText(footerText)

    // A short page still has the footer at the bottom of the window, on a phone too, where the sidebar becomes a bar above the page
    await page.goto('/no/such/page')
    await expect(heading(page)).toHaveText('Page not found')
    await expectAtBottom(page)
    await page.setViewportSize({ width: 390, height: 844 })
    await expectAtBottom(page)
})
