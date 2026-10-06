import { standalone } from './endpoints.svelte'
import type { Scope } from './types'

// Tokens live in session storage only, so they're gone once the tab closes
// In standalone mode each endpoint has its own token, and the tab remembers which endpoint it's connected to
const tokenKey = 'francis-dashboard-token'
const endpointKey = 'francis-dashboard-endpoint'

function storageGet(key: string): string | null {
    try {
        return sessionStorage.getItem(key)
    } catch {
        return null
    }
}

function storageSet(key: string, value: string | null) {
    try {
        if (value === null) {
            sessionStorage.removeItem(key)
        } else {
            sessionStorage.setItem(key, value)
        }
    } catch {
        // Storage can be unavailable, such as in some private windows, and the session then lasts until the page reloads
    }
}

function tokenStorageKey(endpoint: string): string {
    return endpoint ? `${tokenKey}:${endpoint}` : tokenKey
}

class Session {
    // The origin of the management API, an empty string when it's this page's own origin, and null until one is picked in standalone mode
    endpoint = $state<string | null>(standalone ? storageGet(endpointKey) : '')
    // The API token, null when signed out
    token = $state<string | null>(this.endpoint === null ? null : storageGet(tokenStorageKey(this.endpoint)))
    // The scopes the token grants, null until the server reported them
    scopes = $state.raw<ReadonlySet<Scope> | null>(null)
    // Why the session ended, when it wasn't the user's choice
    notice = $state<string | null>(null)

    // A token without any of the scopes that authorize actions can only read
    readOnly = $derived(this.scopes !== null && ![...this.scopes].some((s) => s.endsWith(':manage')))

    can(scope: Scope): boolean {
        return this.scopes?.has(scope) ?? false
    }

    // connect switches to an endpoint, picking up the token this tab already holds for it
    connect(endpoint: string) {
        storageSet(endpointKey, endpoint)
        this.endpoint = endpoint
        this.token = storageGet(tokenStorageKey(endpoint))
        this.scopes = null
        this.notice = null
    }

    // disconnect goes back to picking an endpoint, keeping the tokens of every endpoint for this tab
    disconnect() {
        storageSet(endpointKey, null)
        this.endpoint = null
        this.token = null
        this.scopes = null
        this.notice = null
    }

    start(token: string, scopes: Scope[]) {
        if (this.endpoint === null) {
            return
        }
        storageSet(tokenStorageKey(this.endpoint), token)
        this.token = token
        this.scopes = new Set(scopes)
        this.notice = null
    }

    setScopes(scopes: Scope[]) {
        this.scopes = new Set(scopes)
    }

    end(notice: string | null = null) {
        if (this.endpoint !== null) {
            storageSet(tokenStorageKey(this.endpoint), null)
        }
        this.token = null
        this.scopes = null
        this.notice = notice
    }

    // hasToken reports whether this tab holds a token for an endpoint
    hasToken(endpoint: string): boolean {
        return storageGet(tokenStorageKey(endpoint)) !== null
    }
}

export const session = new Session()
