MultiConsumer Queue
===================

A wrapper to build multi-consumer queues: tasks may be consumed by multiple processor groups, identified by an id.


## Implementations

  * [MultiConsumer Kue](https://npmjs.com/package/multiconsumer-kue) - implementation using [Kue](https://www.npmjs.com/package/kue) (_COMING SOON_)
  * [MultiConsumer Bull](https://npmjs.com/package/multiconsumer-bull) - implementation using [Bull](https://www.npmjs.com/package/bull) (_COMING SOON_)

## Design

Library provides few classes and defines a set of interface which basically helps dispatching
 all jobs from input `Queue<Job>` to output `NamedQueue<Job>` using consumer `groupId`
 as output topic name.

When new job processor is attached, specified `groupId` is added to a [`RedisLiveSet<string>`](https://www.npmjs.com/package/redis-liveset)
which is synchronized across all nodes.

Ex: you're pushing you're submitting `doSomethingUseful` named kue tasks, and have 2 different processors: `log` and `save` -
wrapper will dispatch `doSomethingUseful` job data as `doSomethingUseful/log` and `doSomethingUseful/save` tasks
and will attach your handlers to those names.

Implementation consist of writing a `NamedQueue<Job>` for your queue backend:

```typescript
import * as kue from "kue"
import { NamedQueue, ProcessCallback } from "multiconsumer-queue"
import { createStringsLiveSet } from "redis-liveset"

export class KueNamedQueue implements NamedQueue<kue.Job> {
  constructor(private readonly _out: kue.Queue) {
  }

  add(name: string, data: any): void {
    this._out.create(name, data).removeOnComplete(true).save()
  }

  process(name: string, fn: ProcessCallback<kue.Job>, n: number = 1): void {
    this._out.process(name, n, fn)
  }
}
```

Then you can use it to new `MultiConsumerQueueImpl<Job>` instance,
 which implements consumer groups synchronization routine:

```typescript
import * as kue from "kue"
import { EventBus, Queue, NamedQueue, MultiConsumerQueueImpl, NamedQueue, ProcessCallback } from "multiconsumer-queue"


export function createEventBus(queue: kue.Queue, redis: () => redis.RedisClient): EventBus<kue.Job> {
  return new EventBusImpl((topic: string) => {
    const source: Queue<kue.Job> = new NamedQueueWrap(topic, queue)
    const out: NamedQueue<kue.Job> = new DynamicallyNamedQueue(
      (groupId: string) => `${topic}/${groupId}`,
      new KueNamedQueue(queue)      
    )
  
    return new MultiConsumerQueueImpl(
      source,
      out,
      createStringsLiveSet(`queueConsumerGroups/${topic}`, redis(), redis()),
      (job) => job.data // this function extracts data from input job, to be passed to output queues
    )
  })
}
```

And now you can use it:

```typescript
import * as kue from "kue"
import * as redis from "redis"

const bus = createEventBus(kue.createQueue(), () => redis.createClient())

// Process "my-topic" for logging
bus.topic("my-topic").process("log", (job, cb) => {
  console.log("got new job in topic \"my-topic\" with data", job.data)
  cb()
})

// Save all "my-topic" messages to database
bus.topic("my-topic").process("save", (job, cb) => {
  // here we're going to save all messages from "my-topic" to database
  cb()
})

bus.add("Hello World!")
```

NOTE: Wrapper implementation is not removing consumer groups from `RedisLiveQueue` so once you're
 not interested anymore for processing topic messages for specific `groupId` -
 you must remove that group and tasks manually

Group can be removed using `MultiConsumerQueueImpl.removeGroup()` method:

```typescript

const bus = createEventBus(...)

// deploy this to your servers to stop collecting tasks
bus.topic("my-topic").removeGroup("old-process-group")

```

You will still have to manually remove tasks already added for that group, or maybe those may expire, 
this depends on how source `NamedQueue` is implemented.


## Contribute

> Perfection is Achieved Not When There Is Nothing More to Add, 
> But When There Is Nothing Left to Take Away

Fork, Contribute, Push, Create pull request, Thanks. 


## License

All code in this repository is licensed under the Apache License, Version 2.0. See [LICENCE](./LICENSE).
