import { Database } from 'bun:sqlite'

const queuePollInterval = process.env.QUEUE_POLL_INTERVAL ? Number(process.env.QUEUE_POLL_INTERVAL) : 60000
const jobsBatchInterval = process.env.JOBS_BATCH_INTERVAL ? Number(process.env.JOBS_BATCH_INTERVAL) : 1000
const jobsBatchSize = process.env.JOBS_BATCH_SIZE ? Number(process.env.JOBS_BATCH_SIZE) : 6
const maxJobsInProgress = process.env.MAX_JOBS_IN_PROGRESS ? Number(process.env.MAX_JOBS_IN_PROGRESS) : 20
const hostname = process.env.USE_LOCAL_HOST ? 'localhost' : '[::]'
export const port = process.env.PORT ? Number(process.env.PORT) : 8080

interface HttpJob {
    id?: number
    method: 'GET' |'POST' | 'PUT' | 'PATCH' | 'OPTIONS' | 'DELETE'
    url: string
    headers?: string // must be valid JSON
    body?: string
    executionTime: number
    retry: number
    processed?: boolean
    directToQueue?: boolean
}

const db = new Database(process.cwd() + '/sqlite/db.sqlite')

db.run('PRAGMA journal_mode = WAL;')
db.run(`CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  method VARCHAR(10) NOT NULL,
  url NVARCHAR(2048) NOT NULL,
  headers TEXT CHECK((headers IS NULL) OR (json_valid(headers))),
  body TEXT,
  executionTime BIGINT NOT NULL,
  retry INTEGER,
  processed BOOLEAN NOT NULL,
  responseInfo TEXT
);`)
db.run('CREATE INDEX IF NOT EXISTS jobs_index ON jobs (executionTime, processed);')

const _insertJob = db.query('INSERT into jobs (method, url, headers, body, executionTime, retry, processed) VALUES ($method, $url, $headers, $body, $executionTime, $retry, $processed);')

const _updateProcessedJob = db.query('UPDATE jobs SET processed = 1, responseInfo = ?1 WHERE id = ?2;')

const insertJob = (job: HttpJob) => _insertJob.run({
  $method: job.method,
  $url: job.url,
  $headers: job.headers || null,
  $body: job.body || null,
  $executionTime: job.executionTime,
  $retry: job.retry,
  $processed: job.processed || false
})

const updateProcessedJob = (message: string, id: number) => _updateProcessedJob.run(message, id) 

const getUnprocessedJobs = (...idsToExclude: number[]) => {
  return db
    .query<HttpJob, number>(`SELECT id, url, method, headers, body, retry FROM jobs WHERE processed = 0 AND executionTime <= ?1 AND id NOT IN (${idsToExclude.join(', ')});`)
    .all(Date.now())
}

const jobsInProgress: Set<number> = new Set()

export const runHttpJob = async (job: HttpJob) => {
  const start = performance.now()
  let message = ''

  for (let i = 0; i <= (job.retry || 0); i++) {
    try {
      if (i) await new Promise(resolve => setTimeout(resolve, 2 ** i * 1000))

      const res = await fetch(job.url, {
        method: job.method,
        headers: job.headers ? JSON.parse(job.headers) : undefined,
        body: job.body
      })

      message = `${res.status} ${res.statusText}`
      break
    } catch (e) {
      message = `ERR "${(e as Error).message}"`
    }
  }

  if (job.directToQueue) {
    job.processed = true
    insertJob(job)
  } else {
    updateProcessedJob(message, job.id!)
  }

  if (job.id !== undefined) jobsInProgress.delete(job.id)
  
  console.log(job.method, job.url, message, `${Math.round(performance.now() - start)}ms`)
}

interface JobNode {
    value: HttpJob | undefined
    next: JobNode | undefined
}

const EMPTY_NODE = {} as JobNode

const ITER_DONE = {
    value: undefined,
    done: true
} as const

const queue = {
    head: EMPTY_NODE,
    tail: EMPTY_NODE,
    length: 0,
    enqueue(...items: HttpJob[]) {
        for (const job of items) {
            this.length++

            if (this.head === this.tail) {
                if (!this.head.value) {
                    this.head.value = this.tail.value = job
                    continue
                }

                this.head.next = this.tail = {
                    value: job,
                    next: undefined
                }
                continue
            }

            this.tail.next = this.tail = {
                value: job,
                next: undefined
            }
        }
    },
    dequeue(): HttpJob | undefined {
        if (this.length > 0) this.length--

        const job = this.head.value

        if (this.head === this.tail) {
            this.head.value = this.tail.value = undefined
            return job
        }

        this.head = this.head.next!
        return job
    },
    ids() {
        let node = this.head as JobNode | undefined

        return {
            [Symbol.iterator]() {
                return {
                    next() {
                        let currentNode = node
                        let job = currentNode?.value
                        let id = job?.id
                        if (!currentNode || !job) return ITER_DONE

                        while (id === undefined) {
                            node = node!.next
                            currentNode = node
                            job = currentNode?.value
                            id = job?.id
                            if (!currentNode || !job) return ITER_DONE
                        }

                        if (node) node = node.next

                        return {
                            value: id,
                            done: currentNode === undefined
                        }
                    }
                }
            }
        }
    }
}

const parseRequestBody = async (req: Request): Promise<HttpJob> => {
    const bodyStr = (await req.body?.getReader()?.readMany())
      ?.value
      ?.reduce((result, current: Uint8Array) => result + String.fromCharCode(...current), '')
    
    if (!bodyStr) throw new Error('No request body')
    return JSON.parse(bodyStr)
}

const startHttpServer = () => {
  Bun.serve({
    hostname,
    port,
    async fetch(req): Promise<Response> {
        if (req.url.endsWith('/jobs')) {
          const jobs = db.query<HttpJob, []>('SELECT * FROM jobs').all()
          return new Response(JSON.stringify(jobs), {
            status: 200,
            statusText: 'OK',
            headers: { 'Content-Type': 'application/json' }
          })
        }

        let job: HttpJob
    
        try {
            job = await parseRequestBody(req)
        } catch(e) {
            return new Response(null, { status: 400, statusText: (e as Error).message })
        }
    
        if (!job.method || !job.url) {
            return new Response(null, { status: 400, statusText: 'No method and/or url provided' })
        }
    
        job.executionTime = job.executionTime || 0
        job.retry = job.retry || 0
        job.processed = false
    
        if (!job.executionTime || job.executionTime <= Date.now()) {
            job.directToQueue = true
            queue.enqueue(job)
        } else {
            insertJob(job)
            console.log('CREATED JOB:', job.method, job.url)
        }
    
        return new Response(null, { status: 204 })
    }
  })

  console.log(`Listening at ${hostname}:${port}`)
}

const enqueueJobs = () => {
  setInterval(() => {
    queue.enqueue(...getUnprocessedJobs(...queue.ids(), ...jobsInProgress.keys()))
  }, queuePollInterval)
}

export const batchAndRunJobs = (runJob: typeof runHttpJob) => {
  setInterval(async () => {
    if (!queue.length || jobsInProgress.size > maxJobsInProgress) return
  
    const batchOfRuns: Promise<void>[] = []
  
    for (let i = 0; i < jobsBatchSize; i++) {
      const job = queue.dequeue()
      if (!job) break
      if (job.id !== undefined && jobsInProgress.has(job.id)) continue
      if (job.id !== undefined) jobsInProgress.add(job.id)
      batchOfRuns.push(runJob(job))
    }
  
    await Promise.all(batchOfRuns)
  }, jobsBatchInterval)
}

startHttpServer()
enqueueJobs()
batchAndRunJobs(runHttpJob)