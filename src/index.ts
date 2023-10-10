import { HttpJob } from './types'
import { getUnprocessedJobs, insertJob, updateProcessedJob } from './db'

const port = process.env.PORT ? Number(process.env.PORT) : 8080
const hostname = process.env.USE_LOCAL_HOST ? 'localhost' : '[::]'
const queuePollInterval = process.env.QUEUE_POLL_INTERVAL ? Number(process.env.QUEUE_POLL_INTERVAL) : 60000
const httpJobQueue: HttpJob[] = []
const jobsInProgress: Set<number> = new Set()

Bun.serve({
  hostname,
  port,
  fetch: async (req): Promise<Response> => {
    const bodyStr = (await req.body?.getReader()?.readMany())
      ?.value
      ?.reduce((result, current: Uint8Array) => result + String.fromCharCode(...current), '')
    
    if (!bodyStr) {
      return new Response(null, { status: 400, statusText: 'No request body' })
    }

    let reqBody: HttpJob

    try {
      reqBody = JSON.parse(bodyStr)
    } catch {
      return new Response(null, { status: 400, statusText: 'Invalid JSON' })
    }

    insertJob.run({
      $method: reqBody.method || 'GET',
      $url: reqBody.url || 'https://example.com',
      $headers: reqBody.headers || null,
      $body: reqBody.body || null,
      $executionTime: reqBody.executionTime || 0,
      $retry: reqBody.retry || null,
      $processed: false
    })

    return new Response(null, { status: 204 })
  }
})

console.log(`Listening at ${hostname}:${port}`)

setInterval(async () => {
  httpJobQueue.push(...getUnprocessedJobs.all(Date.now()))

  while (httpJobQueue.length) {
    const job = httpJobQueue.shift()!
    
    if (jobsInProgress.has(job.id!)) continue
    
    jobsInProgress.add(job.id!)

    try {
      const res = await fetch(job.url, {
        method: job.method,
        headers: job.headers ? JSON.parse(job.headers) : undefined,
        body: job.body || undefined
      })

      updateProcessedJob.run(`${res.status} ${res.statusText}`, job.id!)
      console.log(`Job #${job.id!}`, job.method, job.url, `${res.status} ${res.statusText}`)
    } catch (e) {
      updateProcessedJob.run(`ERR "${(e as Error).message}"`, job.id!)
      console.log(`Job #${job.id!}`, job.method, job.url, (e as Error).message)
    }

    jobsInProgress.delete(job.id!)
  }
}, queuePollInterval)
