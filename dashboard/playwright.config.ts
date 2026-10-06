import process from 'node:process'
import { defineConfig, devices } from '@playwright/test'

import { resolveBrowsers } from './e2e/browsers.mjs'
import { CONTROL_URL, EMBEDDED_URL } from './e2e/cluster.mjs'

const browserProjects = {
    chromium: { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
    firefox: { name: 'firefox', use: { ...devices['Desktop Firefox'] } },
    webkit: { name: 'webkit', use: { ...devices['Desktop Safari'] } },
}

export default defineConfig({
    testDir: './e2e',
    // Every test shares one cluster, and the tests that change it create their own actors, instances, and hosts
    fullyParallel: false,
    workers: 1,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 1 : 0,
    reporter: 'list',
    timeout: 60_000,
    expect: { timeout: 10_000 },
    use: {
        baseURL: EMBEDDED_URL,
        headless: true,
        trace: 'retain-on-failure',
        screenshot: 'only-on-failure',
    },
    projects: resolveBrowsers().map((name) => browserProjects[name as keyof typeof browserProjects]),
    webServer: {
        command: 'node ./e2e/start-cluster.mjs',
        url: `${CONTROL_URL}/ready`,
        reuseExistingServer: !process.env.CI,
        timeout: 180_000,
    },
})
