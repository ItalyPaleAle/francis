import { type QueryParams, withQuery } from './paths'

// The current location, which every page reads its parameters and filters from
export const route = $state({
    path: window.location.pathname,
    search: window.location.search,
})

// Navigation is a history entry plus a state update, so the new page renders in the same frame
// A new page starts at the top, while scroll false keeps the position, for a change to the current page such as a filter
export function navigate(href: string, { replace = false, scroll }: { replace?: boolean; scroll?: boolean } = {}) {
    const url = new URL(href, window.location.origin)
    if (url.pathname === route.path && url.search === route.search) {
        return
    }

    if (replace) {
        history.replaceState(null, '', url)
    } else {
        history.pushState(null, '', url)
    }
    route.path = url.pathname
    route.search = url.search

    if (scroll ?? !replace) {
        window.scrollTo(0, 0)
    }
}

window.addEventListener('popstate', () => {
    route.path = window.location.pathname
    route.search = window.location.search
})

// handleLinkClick turns clicks on same-origin links into client-side navigations
// Clicks that ask for a new tab or window, downloads, and links to the API keep the browser's behavior
export function handleLinkClick(event: MouseEvent) {
    if (
        event.defaultPrevented ||
        event.button !== 0 ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey
    ) {
        return
    }

    const anchor = (event.target as Element | null)?.closest?.('a')
    if (!anchor?.href || anchor.target || anchor.hasAttribute('download')) {
        return
    }

    const url = new URL(anchor.href)
    if (url.origin !== window.location.origin || url.pathname.startsWith('/api/')) {
        return
    }

    event.preventDefault()
    navigate(url.pathname + url.search)
}

// setQuery navigates to the current path with the given query parameters, keeping the scroll position since only the filters change
export function setQuery(params: QueryParams) {
    navigate(withQuery(route.path, params), { scroll: false })
}
