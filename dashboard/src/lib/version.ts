import { formatVersion } from './format'

// version is the version of Francis behind the page, read from the tag the server or the hosted build fills in
// It's empty in the dev server, which fills in nothing
export const version = formatVersion(
    document.querySelector<HTMLMetaElement>('meta[name="francis-version"]')?.content.trim() ?? ''
)
