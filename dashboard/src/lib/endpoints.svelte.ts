// The server rewrites the page's mode tag: "embedded" when it also serves the management API, "standalone" when the dashboard runs on its own
// The dev server runs in standalone mode with "vite --mode standalone"
function readMode(): 'embedded' | 'standalone' {
    if (import.meta.env.MODE === 'standalone') {
        return 'standalone'
    }

    const meta = document.querySelector<HTMLMetaElement>('meta[name="francis-dashboard-mode"]')
    return meta?.content === 'standalone' ? 'standalone' : 'embedded'
}

// standalone is true when the dashboard isn't served by a management API, so its users pick the endpoint to connect to
export const standalone = readMode() === 'standalone'

export interface Endpoint {
    // url is the origin of the management API, such as "http://10.0.0.5:7401"
    url: string
    name: string
}

// Saved endpoints stay in local storage, so they're there in every tab and after the browser restarts
const storageKey = 'francis-dashboard-endpoints'

function readEndpoints(): Endpoint[] {
    try {
        const parsed: unknown = JSON.parse(localStorage.getItem(storageKey) ?? '[]')
        if (!Array.isArray(parsed)) {
            return []
        }
        return parsed.filter((e): e is Endpoint => typeof e?.url === 'string' && typeof e?.name === 'string')
    } catch {
        return []
    }
}

class Endpoints {
    list = $state<Endpoint[]>(standalone ? readEndpoints() : [])

    // save adds the endpoint, or renames it when it's already saved
    save(url: string, name: string) {
        const existing = this.list.find((e) => e.url === url)
        if (existing) {
            existing.name = name
        } else {
            this.list.push({ url, name })
        }
        this.persist()
    }

    remove(url: string) {
        const i = this.list.findIndex((e) => e.url === url)
        if (i >= 0) {
            this.list.splice(i, 1)
            this.persist()
        }
    }

    // label is the name the user gave the endpoint, or its address
    label(url: string): string {
        return this.list.find((e) => e.url === url)?.name || url.replace(/^https?:\/\//, '')
    }

    private persist() {
        try {
            localStorage.setItem(storageKey, JSON.stringify(this.list))
        } catch {
            // Without storage the endpoints last until the page reloads
        }
    }
}

export const endpoints = new Endpoints()
