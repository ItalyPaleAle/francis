import {
    expect,
    filterButton,
    filterOption,
    heading,
    hostID,
    pickOnly,
    refreshUntil,
    startDrainableHost,
    test,
    useToken,
    utcTime,
} from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

test('a host can be drained, with force when it is the last server of a type', async ({ page }) => {
    const host = await startDrainableHost()
    try {
        await page.goto(`/hosts/${host.hostId}`)
        await expect(heading(page)).toHaveText(host.hostId)
        await expect(page.getByRole('link', { name: host.actorType, exact: true })).toBeVisible()

        await page.getByRole('button', { name: 'Drain', exact: true }).click()
        const dialog = page.getByRole('dialog', { name: 'Drain host' })
        await dialog.getByLabel('Reason').fill('drained by the e2e tests')

        // The host is the only server of its actor type, so the drain is refused until it's forced
        await dialog.getByRole('button', { name: 'Drain host' }).click()
        await expect(dialog.getByText(`This is the last host serving ${host.actorType}.`)).toBeVisible()

        await dialog.getByLabel('Drain even if this is the last host serving an actor type').check()
        await dialog.getByRole('button', { name: 'Drain host' }).click()
        await expect(page.getByText(`${host.hostId} accepted the drain.`)).toBeVisible()
        await expect(page.getByText(`No live host serves ${host.actorType} anymore.`)).toBeVisible()

        // The drained host shuts down and unregisters, and its page then says so
        expect(await host.exited).toBe(0)
        await refreshUntil(page, async () => {
            await expect(page.getByText(`host '${host.hostId}' is not registered`)).toBeVisible({ timeout: 1000 })
        })
        await expect(page.getByRole('button', { name: 'Drain', exact: true })).toHaveCount(0)
    } finally {
        host.process.kill()
    }
})

test('tables show the last group of a UUID, and dates show the exact time in UTC on hover', async ({ page }) => {
    const id = await hostID()
    await page.goto('/hosts')

    // The link is still named by the whole ID, which also shows on hover
    const link = page.getByRole('link', { name: id, exact: true })
    await expect(link.locator('[aria-hidden="true"]')).toHaveText(`…-${id.slice(-12)}`)
    await expect(link.locator('[title]')).toHaveAttribute('title', id)
    const row = page.getByRole('row').filter({ has: link })
    await expect(row.locator('time')).toHaveText(/ago$|^now$/)
    await expect(row.locator('time')).toHaveAttribute('title', utcTime)

    // The host's page shows the whole ID, and its dates in the browser's time zone with the same hover
    await link.click()
    await expect(heading(page)).toHaveText(id)
    const checked = page.locator('div:has(> dt:text-is("Last health check")) > dd time')
    await expect(checked).not.toHaveText(/ago$/)
    await expect(checked).toHaveAttribute('title', utcTime)
})

test('the activations of a host filter by the actor types it serves, as soon as one is picked', async ({ page }) => {
    const id = await hostID()
    await page.goto(`/hosts/${id}`)
    await expect(filterButton(page, 'Actor type')).toHaveText('All actor types')

    await pickOnly(page, 'Actor type', 'counter')
    await expect(page).toHaveURL(`/hosts/${id}?type=counter`)
    const activations = page.locator('section', { hasText: 'in memory' })
    await expect(activations.getByRole('link', { name: 'user-00', exact: true })).toBeVisible()
    await expect(activations.getByRole('cell', { name: /^francis\.builtin\./ })).toHaveCount(0)

    // A second type joins the first, and checking every type turns the filter off
    await filterButton(page, 'Actor type').click()
    await filterOption(page, 'Actor type', 'francis.builtin.workflow.checkout').click()
    await expect(page).toHaveURL(`/hosts/${id}?type=counter&type=francis.builtin.workflow.checkout`)
    await filterOption(page, 'Actor type', 'All actor types').click()
    await expect(page).toHaveURL(`/hosts/${id}`)
})
