import queue from './queue'
import { getUnprocessedJobs, insertJob, updateProcessedJob } from './db'
import { HttpJob } from './types'

const DEFAULTS = {
  queuePollInterval: 60000,
  jobsBatchInterval: 1000,
  jobsBatchSize: 6,
  maxJobsInProgress: 20
}
const port = process.env.PORT ? Number(process.env.PORT) : 8080
const hostname = process.env.USE_LOCAL_HOST ? 'localhost' : '[::]'
const queuePollInterval = process.env.QUEUE_POLL_INTERVAL ? Number(process.env.QUEUE_POLL_INTERVAL) : DEFAULTS.queuePollInterval
const jobsBatchInterval = process.env.JOBS_BATCH_INTERVAL ? Number(process.env.JOBS_BATCH_INTERVAL) : DEFAULTS.jobsBatchInterval
const jobsBatchSize = process.env.JOBS_BATCH_SIZE ? Number(process.env.JOBS_BATCH_SIZE) : DEFAULTS.jobsBatchSize
const maxJobsInProgress = process.env.MAX_JOBS_IN_PROGRESS ? Number(process.env.MAX_JOBS_IN_PROGRESS) : DEFAULTS.maxJobsInProgress
const jobsInProgress: Set<number> = new Set()

const runHttpJob = async (job: HttpJob) => {
  const start = performance.now()
  let message = ''

  try {
    const res = await fetch(job.url, {
      method: job.method,
      headers: job.headers ? JSON.parse(job.headers) : undefined,
      body: job.body || undefined
    })

    message = `${res.status} ${res.statusText}`
  } catch (e) { // TODO: implement retry strategy(ies)
    message = `ERR "${(e as Error).message}"`
  }

  if (job.directToQueue) {
    insertJob.run({
      $method: job.method,
      $url: job.url,
      $headers: job.headers || null,
      $body: job.body || null,
      $executionTime: job.executionTime,
      $retry: job.retry || null,
      $processed: true
    })
  } else {
    updateProcessedJob.run(message, job.id!)
  }

  if (job.id !== undefined) {
    jobsInProgress.delete(job.id)
  }
  
  console.log(job.method, job.url, message, `${Math.round(performance.now() - start)}ms`)
}

Bun.serve({
  hostname,
  port,
  fetch: async (req): Promise<Response> => {
    let reqBody: HttpJob
    const bodyStr = (await req.body?.getReader()?.readMany())
      ?.value
      ?.reduce((result, current: Uint8Array) => result + String.fromCharCode(...current), '')
    
    if (!bodyStr) {
      return new Response(null, { status: 400, statusText: 'No request body' })
    }

    try {
      reqBody = JSON.parse(bodyStr)
    } catch {
      return new Response(null, { status: 400, statusText: 'Invalid JSON' })
    }

    if (!reqBody.executionTime || reqBody.executionTime <= Date.now()) {
      queue.enqueue({
        method: reqBody.method || 'GET',
        url: reqBody.url || 'https://example.com',
        headers: reqBody.headers,
        body: reqBody.body,
        executionTime: reqBody.executionTime || 0,
        retry: reqBody.retry,
        processed: false,
        directToQueue: true
      })
    } else {
      insertJob.run({
        $method: reqBody.method || 'GET',
        $url: reqBody.url || 'https://example.com',
        $headers: reqBody.headers || null,
        $body: reqBody.body || null,
        $executionTime: reqBody.executionTime || 0,
        $retry: reqBody.retry || null,
        $processed: false
      })

      console.log('CREATED JOB: ', reqBody.method, reqBody.url)
    }

    return new Response(null, { status: 204 })
  }
})

console.log(`Listening at ${hostname}:${port}`)

setInterval(() => {
  queue.enqueue(...getUnprocessedJobs(...queue.ids(), ...jobsInProgress.keys()))
}, queuePollInterval)

setInterval(async () => {
  if (jobsInProgress.size > maxJobsInProgress) return

  const batchOfRuns: Promise<void>[] = []

  for (let i = 0; i < jobsBatchSize; i++) {
    const job = queue.dequeue()
    if (!job) break
    if (!!job.id && jobsInProgress.has(job.id)) continue
    if (!!job.id) jobsInProgress.add(job.id)
    batchOfRuns.push(runHttpJob(job))
  }

  if (!batchOfRuns.length) return
  await Promise.all(batchOfRuns)
}, jobsBatchInterval)