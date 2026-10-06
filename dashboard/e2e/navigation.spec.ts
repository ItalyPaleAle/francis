import { expect, filterButton, heading, pickOnly, test, useToken } from './helpers'

test.beforeEach(async ({ page }) => {
    await useToken(page)
})

const pages = [
    { link: 'Hosts', heading: 'Hosts', path: '/hosts' },
    { link: 'Runtimes', heading: 'Runtime replicas', path: '/runtimes' },
    { link: 'Types', heading: 'Actor types', path: '/actor-types' },
    { link: 'Activations', heading: 'Activations', path: '/activations' },
    { link: 'Placements', heading: 'Placements', path: '/placements' },
    { link: 'Stored state', heading: 'Stored state', path: '/actor-states' },
    { link: 'Jobs', heading: 'Jobs', path: '/jobs' },
    { link: 'Alarms', heading: 'Alarms', path: '/alarms' },
    { link: 'Workflows', heading: 'Workflows', path: '/workflows' },
    { link: 'Overview', heading: 'Overview', path: '/' },
]

test('the sidebar reaches every section without reloading the page', async ({ page }) => {
    await page.goto('/')
    await expect(heading(page)).toHaveText('Overview')

    // A marker on the window survives client-side navigations, but not a page load
    await page.evaluate(() => {
        ;(window as unknown as { marker: number }).marker = 42
    })

    const nav = page.getByRole('navigation', { name: 'Main' })
    for (const p of pages) {
        await nav.getByRole('link', { name: p.link, exact: true }).click()
        await expect(heading(page)).toHaveText(p.heading)
        await expect(page).toHaveURL(p.path)
        await expect(page).toHaveTitle(`${p.heading} · Francis`)
        await expect(nav.getByRole('link', { name: p.link, exact: true })).toHaveAttribute('aria-current', 'page')
    }

    expect(await page.evaluate(() => (window as unknown as { marker?: number }).marker)).toBe(42)
})

test('deep links load directly', async ({ page }) => {
    await page.goto('/workflows/checkout/instances/order-completed')
    await expect(heading(page)).toHaveText('order-completed')
    await expect(page.getByRole('link', { name: 'checkout', exact: true }).first()).toBeVisible()
})

test('an unknown path shows a not-found page inside the shell', async ({ page }) => {
    await page.goto('/no/such/page')
    await expect(heading(page)).toHaveText('Page not found')
    await page.getByRole('link', { name: 'Go to the overview', exact: true }).click()
    await expect(heading(page)).toHaveText('Overview')
})

test('filters live in the URL, so back and forward restore them', async ({ page }) => {
    await page.goto('/jobs')
    await pickOnly(page, 'Status', 'dead')
    await expect(page).toHaveURL('/jobs?status=dead')

    await page.goBack()
    await expect(page).toHaveURL('/jobs')
    await expect(filterButton(page, 'Status')).toHaveText('All statuses')

    await page.goForward()
    await expect(page).toHaveURL('/jobs?status=dead')
    await expect(filterButton(page, 'Status')).toHaveText('dead')
})

test('a page visited before shows its data at once on return', async ({ page }) => {
    await page.goto('/hosts')
    await expect(page.getByText('connected', { exact: true }).first()).toBeVisible()

    // Hold back the reload, so only the cached data can be on screen
    await page.route('**/api/v1/hosts*', async (route) => {
        await new Promise((r) => setTimeout(r, 3000))
        await route.continue().catch(() => {
            // The page may have closed in the meantime
        })
    })
    const nav = page.getByRole('navigation', { name: 'Main' })
    await nav.getByRole('link', { name: 'Alarms', exact: true }).click()
    await expect(heading(page)).toHaveText('Alarms')
    await nav.getByRole('link', { name: 'Hosts', exact: true }).click()
    await expect(page.getByText('connected', { exact: true }).first()).toBeVisible({ timeout: 1000 })
})

test('no page scrolls sideways at phone width', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 })
    for (const path of ['/', '/hosts', '/activations', '/jobs', '/workflows/checkout', '/actors/counter/user-01']) {
        await page.goto(path)
        await expect(heading(page)).toBeVisible()
        await page.waitForLoadState('networkidle')
        const overflow = await page.evaluate(
            () => document.documentElement.scrollWidth - document.documentElement.clientWidth
        )
        expect(overflow, path).toBe(0)
    }

    // The sidebar becomes a menu
    await page.getByRole('button', { name: 'Open menu' }).click()
    await page.getByRole('navigation', { name: 'Main' }).getByRole('link', { name: 'Jobs', exact: true }).click()
    await expect(heading(page)).toHaveText('Jobs')
    await expect(page.getByRole('navigation', { name: 'Main' })).toBeHidden()
})

test('the colors follow the system theme', async ({ page }) => {
    // The lightness of the page background, from 0 to 1, whether the browser reports it as oklch() or rgb()
    const lightness = () =>
        page.evaluate(() => {
            const color = getComputedStyle(document.body).backgroundColor
            const [first = '0', second = '0', third = '0'] = color.match(/[\d.]+%?/g) ?? []
            if (color.startsWith('oklch')) {
                return first.endsWith('%') ? Number.parseFloat(first) / 100 : Number.parseFloat(first)
            }
            return (Number.parseFloat(first) + Number.parseFloat(second) + Number.parseFloat(third)) / 3 / 255
        })

    await page.emulateMedia({ colorScheme: 'light' })
    await page.goto('/')
    await expect(heading(page)).toBeVisible()
    expect(await lightness()).toBeGreaterThan(0.9)

    await page.emulateMedia({ colorScheme: 'dark' })
    expect(await lightness()).toBeLessThan(0.2)
})
