import { expect, filterButton, filterOption, heading, pickOnly, test, useToken } from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

test('jobs filter by status, and a job page shows why it failed', async ({ page }) => {
    await page.goto('/jobs')
    await pickOnly(page, 'Status', 'dead')
    await expect(page).toHaveURL('/jobs?status=dead')

    const row = page.getByRole('row', { name: /rebuild/ })
    await expect(row.getByText('dead', { exact: true })).toBeVisible()
    await row.getByRole('link').first().click()

    await expect(page.locator('main header').getByText('dead', { exact: true })).toBeVisible()
    await expect(page.getByText('Last error')).toBeVisible()
    await expect(page.getByText(/does not implement the Job method/)).toBeVisible()
    await page.getByRole('link', { name: 'Jobs', exact: true }).first().click()
    await expect(heading(page)).toHaveText('Jobs')
})

test('the status filter takes several statuses, and every status turns it off', async ({ page }) => {
    await page.goto('/jobs')
    await expect(filterButton(page, 'Status')).toHaveText('All statuses')
    await pickOnly(page, 'Status', 'dead')

    // Checking a second status applies at once, and the statuses keep their lifecycle order
    await filterButton(page, 'Status').click()
    await filterOption(page, 'Status', 'completed').click()
    await expect(page).toHaveURL('/jobs?status=completed&status=dead')
    await expect(filterButton(page, 'Status')).toHaveText('completed, dead')
    await expect(page.getByRole('row', { name: /rebuild/ })).toBeVisible()

    // The last checked status can't be unchecked, since nothing would match
    await filterOption(page, 'Status', 'completed').click()
    await expect(page).toHaveURL('/jobs?status=dead')
    await expect(
        page.getByRole('group', { name: 'Status' }).getByRole('checkbox', { name: 'dead', exact: true })
    ).toBeDisabled()

    await filterOption(page, 'Status', 'All statuses').click()
    await expect(page).toHaveURL('/jobs')
})

test('the actor ID filter needs an actor type', async ({ page }) => {
    await page.goto('/jobs')
    await expect(filterButton(page, 'Actor ID')).toBeDisabled()
    await expect(page.getByRole('button', { name: 'Apply' })).toHaveCount(0)

    // The actor IDs to pick from are the ones the type's jobs belong to
    await pickOnly(page, 'Actor type', 'counter')
    await pickOnly(page, 'Actor ID', 'user-03')
    await pickOnly(page, 'Status', 'pending')
    await expect(page).toHaveURL('/jobs?type=counter&id=user-03&status=pending')
    await expect(page.getByRole('row', { name: /increment/ })).toBeVisible()
})

test('alarms list the seeded alarms with their schedule', async ({ page }) => {
    await page.goto('/alarms')
    const tick = page.getByRole('row', { name: /tick/ })
    await expect(tick.getByText('PT1H')).toBeVisible()
    await expect(page.getByRole('row', { name: /reminder/ })).toBeVisible()

    await pickOnly(page, 'Actor type', 'counter')
    await pickOnly(page, 'Actor ID', 'user-02')
    await expect(page).toHaveURL('/alarms?type=counter&id=user-02')
    await expect(page.getByRole('row', { name: /reminder/ })).toBeVisible()
    await expect(page.getByRole('row', { name: /tick/ })).toHaveCount(0)
})
