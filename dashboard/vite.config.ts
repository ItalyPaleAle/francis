import { readdirSync, readFileSync, rmSync } from 'node:fs'
import path from 'node:path'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig, type Plugin, type PluginOption } from 'vite'
import { VitePWA } from 'vite-plugin-pwa'
import sri from 'vite-plugin-sri-gen'

const outDir = path.resolve(import.meta.dirname, 'dist')

// The hosted build has its own directory, so it never ends up embedded in the runtime
const hostedOutDir = path.resolve(import.meta.dirname, 'dist-hosted')

// The Go package embeds dist/, so the placeholder that keeps it in the source tree must survive a build
// Vite's own emptyOutDir would delete it, so this plugin empties the directory instead
function cleanOutDir(): Plugin {
    return {
        name: 'clean-out-dir',
        apply: 'build',
        buildStart() {
            for (const entry of readdirSync(outDir)) {
                if (entry !== '.gitkeep') {
                    rmSync(path.join(outDir, entry), { recursive: true, force: true })
                }
            }
        },
    }
}

// hostedVersion fills in the page's version tag, since no runtime serves the hosted build to rewrite it
function hostedVersion(version: string): Plugin {
    const escaped = version.replace(/[&<>"]/g, (c) => `&#${c.charCodeAt(0)};`)
    return {
        name: 'hosted-version',
        transformIndexHtml(html) {
            return html.replace(
                /<meta name="francis-version" content="[^"]*"/,
                `<meta name="francis-version" content="${escaped}"`
            )
        },
    }
}

// vercelHeaders returns the headers that vercel.json sets on every path of the hosted dashboard
// The preview server sends them too, so the Playwright tests run the hosted build under the Content-Security-Policy it's deployed with
function vercelHeaders(): Record<string, string> {
    const config: { headers: { source: string; headers: { key: string; value: string }[] }[] } = JSON.parse(
        readFileSync(path.resolve(import.meta.dirname, 'vercel.json'), 'utf8')
    )
    const all = config.headers.find((h) => h.source == '/(.*)')
    return Object.fromEntries((all?.headers ?? []).map((h) => [h.key, h.value]))
}

export default defineConfig(({ mode }) => {
    // The hosted build is the standalone dashboard as a PWA, deployed on its own at my.gofrancis.dev
    const isHosted = mode == 'hosted'
    const isProduction = mode == 'production' || isHosted

    // Additional plugins, which may not be used
    const additionalPlugins: PluginOption[] = []

    if (isHosted) {
        // The hosted build names the version it's built as, which matches the runtime's own default when it isn't set
        additionalPlugins.push(hostedVersion(process.env.FRANCIS_VERSION || 'canary'))
        additionalPlugins.push(
            VitePWA({
                registerType: 'autoUpdate',
                // The page's Content-Security-Policy forbids inline scripts, so the service worker is registered from a file
                injectRegister: 'script-defer',
                workbox: {
                    // Include the compiled files and assets
                    // We exclude apple-touch-icon and icon* because those are added by the page too
                    globPatterns: ['**/*.{js,css,html,svg,png,ico,woff,woff2}'],
                    globIgnores: ['**/*.map', '**/manifest*.json', '**/icon*.png', '**/apple-touch-icon.png'],
                    sourcemap: true,
                    // Inline the Workbox runtime into the sw.js file
                    inlineWorkboxRuntime: true,
                    // Disable navigation fallback so SW only serves exact precached URLs
                    navigateFallback: null,
                    // No runtime caching - only serve precached assets
                    runtimeCaching: [],
                    // Clean up old caches from previous versions
                    cleanupOutdatedCaches: true,
                    // Force immediate activation of new service worker
                    skipWaiting: true,
                    clientsClaim: true,
                },
                // PWA manifest
                manifest: {
                    name: 'Francis',
                    short_name: 'Francis',
                    start_url: '/',
                    display: 'standalone',
                    background_color: '#edf2f7',
                    theme_color: '#edf2f7',
                    orientation: 'portrait',
                    icons: [],
                },
            })
        )
    }

    return {
        // The hosted build's directory has no placeholder to keep, so Vite empties it itself
        plugins: [
            !isHosted && cleanOutDir(),
            svelte(),
            tailwindcss(),
            // The page's Content-Security-Policy forbids inline scripts, import maps included, so integrity goes in modulepreload links instead
            sri({ importMapIntegrity: false }),
            ...additionalPlugins,
        ],
        resolve: {
            alias: {
                '$lib': path.resolve(import.meta.dirname, './src/lib'),
                '$components': path.resolve(import.meta.dirname, './src/components'),
                '$pages': path.resolve(import.meta.dirname, './src/pages'),
            },
        },
        test: {
            // The e2e directory holds the Playwright tests, which vitest must not pick up
            exclude: ['node_modules/**', 'dist/**', 'e2e/**'],
        },
        build: {
            outDir: isHosted ? hostedOutDir : outDir,
            emptyOutDir: isHosted,
            sourcemap: !isProduction,
            // The page's Content-Security-Policy allows no data: URLs, so every asset must be a file
            assetsInlineLimit: 0,
            rolldownOptions: {
                output: {
                    entryFileNames: 'assets/[name].[hash].js',
                    chunkFileNames: 'assets/[name].[hash].js',
                    assetFileNames: 'assets/[name].[hash].[ext]',
                },
            },
        },
        preview: {
            headers: isHosted ? vercelHeaders() : undefined,
        },
        server: {
            port: 3000,
            proxy: {
                // Send API requests to a runtime with the management API enabled
                '/api': {
                    target: process.env.FRANCIS_MANAGEMENT_URL || 'http://localhost:7401',
                    changeOrigin: true,
                },
            },
        },
    }
})
