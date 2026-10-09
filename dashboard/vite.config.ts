import { readdirSync, rmSync } from 'node:fs'
import path from 'node:path'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig, type Plugin } from 'vite'
import sri from 'vite-plugin-sri-gen'

const outDir = path.resolve(import.meta.dirname, 'dist')

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

export default defineConfig(({ mode }) => {
    const isProduction = mode === 'production'

    return {
        plugins: [cleanOutDir(), svelte(), tailwindcss(), sri()],
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
            outDir,
            emptyOutDir: false,
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
