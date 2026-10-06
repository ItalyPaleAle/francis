import { MANAGEMENT_TOKEN, READ_ONLY_TOKEN } from './cluster.mjs'
import { expect, hostID, signIn, TOKEN_KEY, test, useToken } from './helpers'

test('the embedded dashboard asks for a token, not an endpoint', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByRole('heading', { name: 'Management dashboard' })).toBeVisible()
    await expect(page.getByLabel('API token')).toBeFocused()
    await expect(page.getByRole('heading', { name: 'Connect to a cluster' })).toHaveCount(0)
})

test('an unknown token is rejected', async ({ page }) => {
    await page.goto('/')
    await page.getByLabel('API token').fill('not-a-token-0123456789abcdefghijklmnopq')
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(page.getByRole('alert')).toHaveText("The server doesn't accept this token.")
    await expect(page.getByLabel('API token')).toHaveAttribute('aria-invalid', 'true')
})

test('the token is kept in session storage only, and signing out removes it', async ({ page }) => {
    await signIn(page)

    expect(await page.evaluate((key) => sessionStorage.getItem(key), TOKEN_KEY)).toBe(MANAGEMENT_TOKEN)
    const local = await page.evaluate(() => JSON.stringify(localStorage))
    expect(local).not.toContain(MANAGEMENT_TOKEN)

    // A reload keeps the session, since the tab is still open
    await page.reload()
    await expect(page.getByRole('heading', { level: 1, name: 'Overview' })).toBeVisible()

    await page.getByRole('button', { name: 'Sign out' }).click()
    await expect(page.getByRole('heading', { name: 'Management dashboard' })).toBeVisible()
    expect(await page.evaluate((key) => sessionStorage.getItem(key), TOKEN_KEY)).toBeNull()
})

test('a token the server stopped accepting ends the session with a notice', async ({ page }) => {
    await useToken(page, 'revoked-token-0123456789abcdefghijklmnopqr')
    await page.goto('/hosts')
    await expect(page.getByRole('status')).toHaveText('The server no longer accepts this token. Sign in again.')
    await expect(page.getByLabel('API token')).toBeVisible()
})

test.describe('with a read-only token', () => {
    test.beforeEach(async ({ page }) => {
        await useToken(page, READ_ONLY_TOKEN)
    })

    test('the dashboard says so and offers no actions', async ({ page }) => {
        await page.goto('/')
        await expect(page.getByText('Read-only token')).toBeVisible()

        await page.goto(`/hosts/${await hostID()}`)
        await expect(page.getByRole('heading', { name: 'Actor types' })).toBeVisible()
        await expect(page.getByRole('button', { name: 'Drain' })).toHaveCount(0)
        await expect(page.getByRole('button', { name: 'Deactivate' })).toHaveCount(0)

        await page.goto('/actors/counter/user-01')
        await expect(page.getByRole('button', { name: 'Read state' })).toBeVisible()
        await expect(page.getByRole('button', { name: 'Deactivate' })).toHaveCount(0)

        await page.goto('/workflows/checkout/instances/order-waiting')
        await expect(page.getByText('running', { exact: true }).first()).toBeVisible()
        for (const name of ['Cancel', 'Suspend', 'Resume']) {
            await expect(page.getByRole('button', { name, exact: true })).toHaveCount(0)
        }
    })

    test('reads that need no management scope still work', async ({ page }) => {
        await page.goto('/actors/counter/user-01')
        await page.getByRole('button', { name: 'Read state' }).click()
        await expect(page.getByText('18446744073709551615')).toBeVisible()

        await page.goto('/workflows/checkout/instances/order-completed')
        await expect(page.getByText('Input', { exact: true })).toBeVisible()
        await expect(page.getByText('"orderId": "order-completed"')).toBeVisible()
    })
})

test('a management token offers the actions', async ({ page }) => {
    await useToken(page)
    await page.goto('/workflows/checkout/instances/order-waiting')
    await expect(page.getByRole('button', { name: 'Suspend', exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toBeVisible()
    await expect(page.getByText('Read-only token')).toHaveCount(0)
})
