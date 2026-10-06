import { SEED_HOST_ADDRESS } from './cluster.mjs'
import { expect, filterButton, heading, hostID, test, useToken } from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

test('the overview summarizes the cluster', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByText(/^Remote topology · observed/)).toBeVisible()
    await expect(page.getByText('Runtime replicas', { exact: true })).toBeVisible()

    // The seeded instances show in the workflow table, linked to the filtered instance list
    const row = page.getByRole('row', { name: /checkout/ })
    await expect(row).toBeVisible()
    await row.getByRole('link', { name: 'checkout', exact: true }).click()
    await expect(heading(page)).toHaveText('checkout')
})

test('the overview links each job status to the filtered list', async ({ page }) => {
    await page.goto('/')
    await page.getByText('Dead', { exact: true }).locator('..').getByRole('link').click()
    await expect(page).toHaveURL('/jobs?status=dead')
    await expect(filterButton(page, 'Status')).toHaveText('dead')
})

test('hosts list their state and link to their page', async ({ page }) => {
    const id = await hostID()
    await page.goto('/hosts')

    const row = page.getByRole('row', { name: new RegExp(id) })
    await expect(row.getByText('connected', { exact: true })).toBeVisible()
    await expect(row.getByText(SEED_HOST_ADDRESS)).toBeVisible()

    // Filtering by a state no host is in shows an empty list
    await page.getByRole('link', { name: 'Unreachable', exact: true }).click()
    await expect(page).toHaveURL('/hosts?state=unreachable')
    await expect(page.getByText('No host is unreachable.')).toBeVisible()

    await page.getByRole('link', { name: 'All', exact: true }).click()
    await page.getByRole('link', { name: id, exact: true }).click()
    await expect(heading(page)).toHaveText(id)
})

test('a host page shows its registration, actor types, and activations', async ({ page }) => {
    const id = await hostID()
    await page.goto(`/hosts/${id}`)

    await expect(page.getByText(SEED_HOST_ADDRESS)).toBeVisible()
    await expect(page.getByRole('link', { name: 'counter', exact: true })).toBeVisible()
    await expect(page.getByRole('cell', { name: 'checkout', exact: true })).toBeVisible()

    // The activations come from the host itself, and filter by type, even one the host doesn't serve when a link names it
    await expect(page.getByRole('link', { name: 'user-01', exact: true })).toBeVisible()
    await page.goto(`/hosts/${id}?type=no-such-type`)
    await expect(filterButton(page, 'Actor type')).toHaveText('no-such-type')
    await expect(page.getByText('No no-such-type actor is active on this host.')).toBeVisible()
})

test('the runtime list marks the replica serving the page', async ({ page }) => {
    await page.goto('/runtimes')
    await expect(page.getByText('serving this page')).toBeVisible()
})
