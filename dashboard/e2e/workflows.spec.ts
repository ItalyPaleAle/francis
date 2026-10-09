import { expect, heading, pageStatus, pickOnly, refreshUntil, startInstance, test, uniqueID, useToken } from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

test('the workflow list links to a workflow with its instances', async ({ page }) => {
    await page.goto('/workflows')
    await page.getByRole('link', { name: 'checkout', exact: true }).click()
    await expect(heading(page)).toHaveText('checkout')
    await expect(page.getByText(/Version 1 is the latest · 1 host serves it/)).toBeVisible()

    for (const [id, status] of [
        ['order-completed', 'completed'],
        ['order-failed', 'failed'],
        ['order-waiting', 'running'],
        ['order-suspended', 'suspended'],
    ]) {
        const row = page.getByRole('row', { name: new RegExp(`^${id} `) })
        await expect(row.getByText(status, { exact: true }), id).toBeVisible()
    }

    // The registry recorded the one version the fixture serves
    const versions = page.locator('section', { hasText: 'Registered versions' })
    await expect(versions.getByRole('row')).toHaveCount(2)
})

test('instances filter by status', async ({ page }) => {
    await page.goto('/workflows/checkout')
    await pickOnly(page, 'Status', 'failed')
    await expect(page).toHaveURL('/workflows/checkout?status=failed')
    await expect(page.getByRole('link', { name: 'order-failed', exact: true })).toBeVisible()
    await expect(page.getByRole('link', { name: 'order-completed', exact: true })).toHaveCount(0)
})

test('the free-form instance filters apply as they are typed', async ({ page }) => {
    await page.goto('/workflows/checkout')
    await expect(page.getByRole('button', { name: 'Apply' })).toHaveCount(0)

    await page.getByLabel('Version').fill('1')
    await expect(page).toHaveURL('/workflows/checkout?version=1')
    await expect(page.getByRole('link', { name: 'order-completed', exact: true })).toBeVisible()

    await page.getByLabel('Parent').fill('no-such-parent')
    await expect(page).toHaveURL('/workflows/checkout?version=1&parent=no-such-parent')
    await expect(page.getByText('No instance matches the filters.')).toBeVisible()
})

test('a completed instance shows its steps, data, and event history', async ({ page }) => {
    await page.goto('/workflows/checkout/instances/order-completed')
    await expect(pageStatus(page)).toHaveText('completed')

    const steps = page.locator('section', { hasText: 'Steps' }).first()
    for (const step of ['reserve-inventory', 'approval', 'charge', 'ship']) {
        await expect(steps.getByRole('row', { name: new RegExp(`^${step}`) })).toBeVisible()
    }
    await expect(page.getByText('"orderId": "order-completed"')).toBeVisible()
    await expect(page.getByText('"shipped"')).toBeVisible()

    const history = page.locator('section', { hasText: 'Event history' })
    await expect(history.getByText('instance_started')).toBeVisible()
    await expect(history.getByText('instance_completed')).toBeVisible()

    // A terminal instance can't be controlled
    for (const name of ['Cancel', 'Suspend', 'Resume']) {
        await expect(page.getByRole('button', { name, exact: true })).toHaveCount(0)
    }
})

test('the timeline shows the steps that ran, and scrolls once zoomed in', async ({ page }) => {
    await page.goto('/workflows/checkout/instances/order-completed')
    const timeline = page.getByRole('region', { name: 'Timeline of the steps' })
    for (const step of ['reserve-inventory', 'approval', 'charge', 'ship']) {
        await expect(timeline.getByText(step, { exact: true })).toBeVisible()
    }

    // The whole timeline fits the frame until it's zoomed in
    const scrolls = () => timeline.evaluate((el) => el.scrollWidth > el.clientWidth)
    expect(await scrolls()).toBe(false)
    await expect(page.getByRole('button', { name: 'Zoom out' })).toBeDisabled()
    await page.getByRole('button', { name: 'Zoom in' }).click()
    await expect.poll(scrolls).toBe(true)
    await expect(page.getByRole('button', { name: 'Zoom out' })).toBeEnabled()

    // A step that hasn't ended runs to now
    await page.goto('/workflows/checkout/instances/order-suspended')
    const suspended = page.getByRole('region', { name: 'Timeline of the steps' })
    await expect(suspended.getByText('Suspended', { exact: true })).toBeVisible()
    await expect(suspended.getByText(/so far$/)).toHaveCount(1)
})

test('a failed instance shows why it failed', async ({ page }) => {
    await page.goto('/workflows/checkout/instances/order-failed')
    await expect(pageStatus(page)).toHaveText('failed')
    await expect(page.getByText('Cause', { exact: true })).toBeVisible()
    await expect(page.getByText(/card declined/).first()).toBeVisible()
    await expect(page.getByRole('row', { name: /^charge/ }).getByText('failed', { exact: true })).toBeVisible()
})

test('a suspended instance shows its suspension', async ({ page }) => {
    await page.goto('/workflows/checkout/instances/order-suspended')
    await expect(pageStatus(page)).toHaveText('suspended')
    await expect(page.getByRole('definition').filter({ hasText: 'waiting on fraud review' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Resume', exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Suspend', exact: true })).toHaveCount(0)
})

test('an instance can be suspended and resumed', async ({ page }) => {
    const id = uniqueID('order-pause')
    await startInstance(id)

    await page.goto(`/workflows/checkout/instances/${id}`)
    await expect(pageStatus(page)).toHaveText('running')

    // Suspending takes an optional reason, which the instance records
    await page.getByRole('button', { name: 'Suspend', exact: true }).click()
    const dialog = page.getByRole('dialog', { name: 'Suspend instance' })
    await dialog.getByLabel('Reason (optional)').fill('paused by the e2e tests')
    await dialog.getByRole('button', { name: 'Suspend instance' }).click()
    await expect(page.getByText('Suspend requested.')).toBeVisible()

    // The engine applies the control job after the request returns
    await refreshUntil(page, async () => {
        await expect(pageStatus(page)).toHaveText('suspended', { timeout: 1000 })
    })
    await expect(page.getByRole('definition').filter({ hasText: 'paused by the e2e tests' })).toBeVisible()

    // Resuming needs no dialog
    await page.getByRole('button', { name: 'Resume', exact: true }).click()
    await expect(page.getByText('Resume requested.')).toBeVisible()
    await refreshUntil(page, async () => {
        await expect(pageStatus(page)).toHaveText('running', { timeout: 1000 })
    })

    const history = page.locator('section', { hasText: 'Event history' })
    await expect(history.getByText('instance_suspended')).toBeVisible()
    await expect(history.getByText('instance_resumed')).toBeVisible()
})

test('cancelling an instance requires a reason', async ({ page }) => {
    const id = uniqueID('order-cancel')
    await startInstance(id)

    await page.goto(`/workflows/checkout/instances/${id}`)
    await page.getByRole('button', { name: 'Cancel', exact: true }).click()
    const dialog = page.getByRole('dialog', { name: 'Cancel instance' })
    const submit = dialog.getByRole('button', { name: 'Cancel instance' })
    await expect(submit).toBeDisabled()

    await dialog.getByLabel('Reason').fill('cancelled by the e2e tests')
    await submit.click()
    await expect(page.getByText('Cancel requested.')).toBeVisible()

    // The instance compensates its completed steps, then ends as cancelled
    await refreshUntil(page, async () => {
        await expect(pageStatus(page)).toHaveText('cancelled', { timeout: 1000 })
    })
    await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toHaveCount(0)
    await expect(page.getByRole('row', { name: /^reserve-inventory/ }).getByText('compensated')).toBeVisible()
})
