import { createBullBoard } from '@bull-board/api'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter'
import { env } from './env'
import { ConnectionOptions, Job, JobsOptions, Queue, QueueScheduler, Worker } from 'bullmq'
import { FastifyAdapter } from '@bull-board/fastify'
import fastify, { FastifyInstance } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'

function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0,
        v = c === 'x' ? r : (r & 0x3 | 0x8)
    return v.toString(16)
  })
}

const QUEUE_NAME = 'HttpRequests'

const connection: ConnectionOptions = {
  host: env.REDISHOST,
  port: env.REDISPORT,
  username: env.REDISUSER,
  password: env.REDISPASSWORD,
}

interface HttpJob {
  method: 'GET' |'POST' | 'PUT' | 'PATCH' | 'OPTIONS' | 'DELETE',
  url: string,
  headers?: Record<string, string>,
  body?: Record<string, unknown>
}

const run = async () => {
  const httpRequestQueue = new Queue<HttpJob, string>(QUEUE_NAME, { connection })
  const queueScheduler = new QueueScheduler(QUEUE_NAME, { connection })
  await queueScheduler.waitUntilReady()

  new Worker(
    QUEUE_NAME,
    async (job: Job<HttpJob, any, string>) => {
      const res = await fetch(job.data.url, {
        method: job.data.method,
        headers: job.data.headers,
        body: job.data.body && JSON.stringify(job.data.body)
      })

      return res.status + ' ' + res.statusText
    },
    { connection }
  )

  const server: FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify()
  const serverAdapter = new FastifyAdapter()
  
  createBullBoard({
    queues: [new BullMQAdapter(httpRequestQueue)],
    serverAdapter
  })

  serverAdapter.setBasePath('/')

  server.register(serverAdapter.registerPlugin(), {
    prefix: '/',
    basePath: '/'
  })

  server.post('/jobs', async (req, reply) => {
    if (!req.body) {
      reply.status(400).send({ error: 'Requests must contain a body.' })
      return
    }

    const body = req.body as { job: HttpJob, options: JobsOptions }
    const job = body.job as HttpJob
    const options = body.options as JobsOptions
    const jobId = uuidv4()

    await httpRequestQueue.add(`http-job-${jobId}`, job, { ...options, jobId })

    reply.send({ ok: true })
  })

  await server.listen({ port: env.PORT, host: '0.0.0.0' })
}

run().catch((e) => {
  console.error(e)
  process.exit(1)
})
