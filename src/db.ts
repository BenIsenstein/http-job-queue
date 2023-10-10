import { Database } from 'bun:sqlite'
import { HttpJob } from './types'

const db = new Database(import.meta.dir + '/../sqlite/db.sqlite')

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

export const insertJob = db.query('INSERT into jobs (method, url, headers, body, executionTime, retry, processed) VALUES ($method, $url, $headers, $body, $executionTime, $retry, $processed);')

export const getUnprocessedJobs = db.query<HttpJob, number>('SELECT * FROM jobs WHERE processed = 0 AND executionTime <= ?1;')

export const updateProcessedJob = db.query('UPDATE jobs SET processed = 1, responseInfo = ?1 WHERE id = ?2;')