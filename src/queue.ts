import { HttpJob } from "./types"

const ITER_DONE = {
    value: undefined,
    done: true
} as const

interface JobNode {
    value: HttpJob | undefined
    next: JobNode | undefined
}

const EMPTY_NODE = {} as JobNode

export default {
    head: EMPTY_NODE,
    tail: EMPTY_NODE,
    enqueue(...items: HttpJob[]) {
        for (const job of items) {
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