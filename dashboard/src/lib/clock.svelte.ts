// A shared clock, so relative times across the page move forward together
export const clock = $state({ now: Date.now() })

setInterval(() => {
    clock.now = Date.now()
}, 15_000)
