export interface HttpJob {
    id?: number
    method: 'GET' |'POST' | 'PUT' | 'PATCH' | 'OPTIONS' | 'DELETE'
    url: string
    headers?: string // must be valid JSON
    body?: string
    executionTime: number
    retry?: number
    processed?: boolean
}