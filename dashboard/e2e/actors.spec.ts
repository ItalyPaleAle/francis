import {
    activateActor,
    chooseOne,
    expect,
    heading,
    hostID,
    pickOnly,
    test,
    uniqueID,
    useToken,
    utcTime,
} from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

test('actor types list the seeded type and link to its page', async ({ page }) => {
    await page.goto('/actor-types')
    await page.getByRole('link', { name: 'counter', exact: true }).click()
    await expect(heading(page)).toHaveText('counter')
    await expect(page.getByText('Completed jobs kept').first()).toBeVisible()

    // The hosts table shows both retentions in one column, completed first, and the fixture keeps dead jobs for a day
    await expect(page.getByRole('columnheader', { name: /Job retention:\s*completed \/ dead/ })).toBeVisible()
    await expect(page.getByRole('cell', { name: /^\S.* \/ 1d$/ }).first()).toBeVisible()

    // The page links to the other lists, filtered by the type
    await page.getByRole('link', { name: 'Stored state', exact: true }).last().click()
    await expect(page).toHaveURL('/actor-states?type=counter')
})

test('activations filter by type and link to actors', async ({ page }) => {
    await page.goto('/activations')
    await pickOnly(page, 'Actor type', 'counter')
    await expect(page).toHaveURL('/activations?type=counter')

    await expect(page.getByRole('link', { name: 'user-00', exact: true })).toBeVisible()
    await expect(page.getByRole('cell', { name: 'order-waiting', exact: true })).toHaveCount(0)

    await page.getByRole('link', { name: 'Clear', exact: true }).click()
    await expect(page).toHaveURL('/activations')
})

test('placements list the actors of a type, and filter by host', async ({ page }) => {
    await page.goto('/placements?type=counter')
    await expect(page.getByRole('link', { name: 'user-02', exact: true })).toBeVisible()

    // The hosts to pick from are the registered ones
    const id = await hostID()
    await pickOnly(page, 'Host', id)
    await expect(page).toHaveURL(`/placements?type=counter&host=${id}`)
    await expect(page.getByRole('link', { name: 'user-02', exact: true })).toBeVisible()
})

test('stored state lists the actors of the chosen type', async ({ page }) => {
    await page.goto('/actor-states')
    await expect(page.getByText('Pick an actor type to list its actors.')).toBeVisible()

    await chooseOne(page, 'Actor type', 'counter')
    await expect(page).toHaveURL('/actor-states?type=counter')
    await page.getByRole('link', { name: 'user-03', exact: true }).click()
    await expect(heading(page)).toHaveText('user-03')
})

test("stored state shows when a workflow's instances were created, relative to now", async ({ page }) => {
    // The fixture creates the instance when the cluster starts, and a time under 5 seconds old reads as "now" until the clock's next tick, so the page looks at it a minute later
    await page.clock.setFixedTime(Date.now() + 60_000)
    await page.goto('/actor-states?type=francis.builtin.workflow.checkout')
    const created = page.getByRole('row', { name: /^order-completed / }).locator('time')
    await expect(created).toHaveText(/ago$/)
    await expect(created).toHaveAttribute('title', utcTime)
})

test("an actor's state is decoded from MessagePack in the browser", async ({ page }) => {
    await page.goto('/actors/counter/user-01')
    await expect(page.getByText('Each read is recorded in the audit log.')).toBeVisible()

    // The state is only read when asked for
    const stateRequest = page.waitForRequest((req) => req.url().includes('/api/v1/actor-states/counter/user-01'))
    await page.getByRole('button', { name: 'Read state' }).click()
    expect((await stateRequest).headers().accept).toBe('application/msgpack')

    const tree = page.locator('details').first()
    await expect(tree).toContainText('map · 8 entries')

    // Values JSON can't represent exactly keep their type and value
    await expect(page.getByText('18446744073709551615')).toBeVisible()
    await expect(page.getByText('2026-01-02T03:04:05.123456789Z')).toBeVisible()
    await expect(page.getByText('de ad be ef')).toBeVisible()
    await expect(page.getByText('bin · 4 bytes')).toBeVisible()
    await expect(page.getByText('float32')).toBeVisible()
    await expect(page.getByText('"minus seven"')).toBeVisible()
    await expect(page.getByText('nil', { exact: true })).toBeVisible()

    // A non-string key is shown with its type
    const byID = page.locator('details', { hasText: 'ByID' }).last()
    await expect(byID).toContainText('-7int')

    // Downloading reuses the bytes already read, so it doesn't read the state again
    let reads = 0
    page.on('request', (req) => {
        if (req.url().includes('/api/v1/actor-states/')) {
            reads++
        }
    })
    const download = page.waitForEvent('download')
    await page.getByRole('button', { name: 'Download' }).click()
    expect((await download).suggestedFilename()).toBe('counter-user-01.msgpack')
    expect(reads).toBe(0)
})

test('an actor without stored state says so', async ({ page }) => {
    await page.goto('/actors/counter/never-activated')
    await page.getByRole('button', { name: 'Read state' }).click()
    await expect(page.getByText('This actor has no stored state.')).toBeVisible()
})

test('the actor page lists its jobs and alarms', async ({ page }) => {
    await page.goto('/actors/counter/user-03')
    await expect(page.getByText('rebuild', { exact: true })).toBeVisible()
    await expect(page.getByRole('cell', { name: 'dead', exact: true })).toBeVisible()

    await page.goto('/actors/counter/user-01')
    await expect(page.getByRole('cell', { name: 'tick', exact: true })).toBeVisible()
})

test('an actor can be deactivated', async ({ page }) => {
    const id = uniqueID('deactivate')
    await activateActor(id)

    await page.goto(`/activations?type=counter`)
    await expect(page.getByRole('link', { name: id, exact: true })).toBeVisible()

    await page.goto(`/actors/counter/${id}`)
    await expect(page.getByRole('button', { name: 'Deactivate' }).locator('svg')).toBeVisible()
    await page.getByRole('button', { name: 'Deactivate' }).click()
    const dialog = page.getByRole('dialog', { name: 'Deactivate actor' })
    await expect(dialog).toContainText(`counter/${id}`)

    // Keeping the actor active closes the dialog without doing anything
    await dialog.getByRole('button', { name: 'Keep active' }).click()
    await expect(dialog).toBeHidden()

    await page.getByRole('button', { name: 'Deactivate' }).click()
    await dialog.getByRole('button', { name: 'Deactivate' }).click()
    await expect(page.getByText(new RegExp(`^${id} was deactivated on `))).toBeVisible()

    // Deactivating it again finds nothing to do
    await page.getByRole('button', { name: 'Deactivate' }).click()
    await dialog.getByRole('button', { name: 'Deactivate' }).click()
    await expect(page.getByText(`${id} wasn't active, so nothing changed.`)).toBeVisible()

    await page.goto(`/activations?type=counter`)
    await expect(page.getByRole('link', { name: 'user-00', exact: true })).toBeVisible()
    await expect(page.getByRole('link', { name: id, exact: true })).toHaveCount(0)
})
