import { standalone } from './endpoints.svelte'
import type { Scope } from './types'

// Tokens live in session storage only, so they're gone once the tab closes
// In standalone mode the tab also remembers which endpoint it's connected to, and the token is always that endpoint's
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

class Session {
    // The origin of the management API, an empty string when it's this page's own origin, and null until one is picked in standalone mode
    endpoint = $state<string | null>(standalone ? storageGet(endpointKey) : '')
    // The API token, null when signed out
    token = $state<string | null>(this.endpoint === null ? null : storageGet(tokenKey))
    // The scopes the token grants, null until the server reported them
    scopes = $state.raw<ReadonlySet<Scope> | null>(null)
    // Why the session ended, when it wasn't the user's choice
    notice = $state<string | null>(null)

    // A token without any of the scopes that authorize actions can only read
    readOnly = $derived(this.scopes !== null && ![...this.scopes].some((s) => s.endsWith(':manage')))

    can(scope: Scope): boolean {
        return this.scopes?.has(scope) ?? false
    }

    // connect picks the endpoint to sign in to
    connect(endpoint: string) {
        storageSet(endpointKey, endpoint)
        storageSet(tokenKey, null)
        this.endpoint = endpoint
        this.token = null
        this.scopes = null
        this.notice = null
    }

    // disconnect goes back to picking an endpoint, dropping the token of the one the tab was connected to
    disconnect() {
        storageSet(endpointKey, null)
        storageSet(tokenKey, null)
        this.endpoint = null
        this.token = null
        this.scopes = null
        this.notice = null
    }

    start(token: string, scopes: Scope[]) {
        if (this.endpoint === null) {
            return
        }
        storageSet(tokenKey, token)
        this.token = token
        this.scopes = new Set(scopes)
        this.notice = null
    }

    setScopes(scopes: Scope[]) {
        this.scopes = new Set(scopes)
    }

    // end drops the token and stays on the endpoint, so its sign-in form can show why the session ended
    end(notice: string | null = null) {
        storageSet(tokenKey, null)
        this.token = null
        this.scopes = null
        this.notice = notice
    }

    // signOut is the user ending the session, which in standalone mode also goes back to picking an endpoint
    signOut() {
        if (standalone) {
            this.disconnect()
        } else {
            this.end()
        }
    }
}

export const session = new Session()
