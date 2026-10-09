export interface Toast {
    id: number
    message: string
    tone: 'neutral' | 'warning'
}

let nextId = 1

// Toasts report the outcome of actions, and dismiss themselves
export const toasts = $state<Toast[]>([])

export function dismissToast(id: number) {
    const i = toasts.findIndex((t) => t.id === id)
    if (i >= 0) {
        toasts.splice(i, 1)
    }
}

export function notify(message: string, tone: Toast['tone'] = 'neutral') {
    const id = nextId++
    toasts.push({ id, message, tone })
    setTimeout(() => dismissToast(id), 6000)
}
