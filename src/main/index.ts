/*
 * Copyright (c) 2017 by The MultiConsumer Queue Project Developers.
 * Some rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createStringsLiveSet, RedisLiveSet } from "redis-liveset"
import { Set, Map } from "immutable"

/**
 * Callback passed to ProcessCallback to be called when processing is done
 */
export type DoneCallback = (error?: Error | null, value?: any) => void

/**
 * Job Processing function interface
 */
export type ProcessCallback<Job> = (job: Job, done: DoneCallback) => void

/**
 * Queue topic name refined type
 */
export type QueueTopic = string & { _tag: "QueueTopic" }

/**
 * QueueTopic constructor
 * @constructor
 */
export function QueueTopic(topic: string): QueueTopic {
  return topic as QueueTopic
}

/**
 * Consumer Group identifier refined type
 */
export type ConsumerGroupId = string & { _tag: "ConsumerGroupId" }

/**
 * ConsumerGroupId constructor
 * @constructor
 */
export function ConsumerGroupId(groupId: string): ConsumerGroupId {
  return groupId as ConsumerGroupId
}

/**
 * {@Link Consumer} accepts messages
 */
export interface Consumer {
  /**
   * Add new Message/Task to queue
   * @param {Object} data
   */
  add(data: any): void
}

/**
 * {@Link Producer} is a source of messages which can be consumed by attaching a callback function
 */
export interface Producer<Job> {
  /**
   * Attach queue processing handler with processing parallelism
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn callback
   * @param {number} n parallelism
   */
  process(fn: ProcessCallback<Job>, n?: number): void
}

/**
 * {@Link Queue} is a {@Link Consumer} and a {@Link Producer}
 */
export interface Queue<Job> extends Producer<Job>, Consumer {

}

/**
 * A {@Link Consumer} which accepts message to different topics identified by a {@Link TopicName}
 */
export interface NamedConsumer {
  add(topic: QueueTopic, data: any): void
}

/**
 * A {@Link Producer} which produces messages to different {@Link QueueTopic}
 */
export interface NamedProducer<Job> {
  /**
   * Attach queue processing handler for given topic name and parallelism
   * @param {QueueTopic} topic
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn
   * @param {number} n
   */
  process(topic: QueueTopic, fn: ProcessCallback<Job>, n?: number): void
}

/**
 * A {@Link Queue} is a {@Link NamedProducer} and a {@Link NamedConsumer}
 */
export interface NamedQueue<Job> extends NamedProducer<Job>, NamedConsumer {

}

/**
 * Defines a {@Link Producer} than can be consumed by multiple groups idenified by {@Link ConsumerGroupId}
 *
 * All distinct groups will process same set of messages
 */
export interface MultiConsumerProducer<Job> {
  /**
   * Process jobs within group defined by groupId identifier
   * @param {ConsumerGroupId} groupId consumer group identifier
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn processing function
   * @param {number} n process parallelism, defaults to 1
   */
  process(groupId: ConsumerGroupId, fn: ProcessCallback<Job>, n?: number): void
}

/**
 * A Queue type which may be consumed by multiple groups
 *
 * It also adds the ability to _unsubscribe_ specific consumer group when it is not going
 *  to consume Queue messages anymore, to avoid storing messages
 */
export interface MultiConsumerQueue<Job> extends Consumer, MultiConsumerProducer<Job> {
  /**
   * Removes consumer group for current topic
   * Use this for cleanup, replace process(groupId) with removeGroup(groupId), deploy for a while
   */
  removeGroup(groupId: ConsumerGroupId): this
}

/**
 * Wraps a {@Link NamedQueue} with given name, exposing {@Link Queue} interface
 */
export class NamedQueueWrap<Job> implements Queue<Job> {
  constructor(private readonly _topic: QueueTopic,
              private readonly _out: NamedQueue<Job>) {
  }

  add(data: any): void {
    this._out.add(this._topic, data)
  }

  process(fn: ProcessCallback<Job>, n?: number): void {
    this._out.process(this._topic, fn, n)
  }
}

/**
 * Passes calls to out {@Link NamedQueue} but transforming input topic name using given map function
 */
export class DynamicallyNamedQueue<Job> implements NamedQueue<Job> {

  constructor(readonly topic: (t: QueueTopic) => QueueTopic,
              readonly out: NamedQueue<Job>) {
  }

  add(topic: QueueTopic, data: any): void {
    this.out.add(this.topic(topic), data)
  }

  process(topic: QueueTopic, fn: ProcessCallback<Job>, n?: number): void {
    this.out.process(this.topic(topic), fn, n)
  }
}

/**
 * EventBus is a communication channel to publish/subscribe for messages by topics
 */
export interface EventBus<Job> {
  /**
   * Returns new EventBus topic queue instance
   */
  topic(topic: QueueTopic): MultiConsumerQueue<Job>
}

/**
 * {@Link MultiConsumerQueue} implementation helper
 *
 * Dispatches messages from source Queue to out {@Link NamedQueue} using all group IDs
 *  (syncronized using a RedisLiveSet) as topic names
 */
export class MultiConsumerQueueImpl<Job> implements MultiConsumerQueue<Job> {
  /**
   * @param _source queue to add and dispatch messages from
   * @param _out a named queue to attach final processors to using groupId as topic name
   * @param _groups live set to synchronize consumer groups across nodes
   * @param _jobDataLens lens to extract data from Job, to support multiple implementations
   */
  constructor(private readonly _source: Queue<Job>,
              private readonly _out: NamedQueue<Job>,
              private readonly _groups: RedisLiveSet<string>,
              private readonly _jobDataLens: (j: Job) => any,
              private _lastGroupsSet: Set<string> = Set()) {
    /**
     * Subscribe for consumer group ids changes
     */
    this._groups.subscribe((consumers) => {
      this._lastGroupsSet = consumers
    })

    // subscribe source processing once groups initialized
    this._groups.onMembersInit(() => {
      // group members list initialized (loaded from db), can now process source jobs
      this._source.process((job, cb) => {
        /**
         * Dispatch messages from source {@Link Queue} to out {@Link NamedQueue} using ConsumerGroupId as QueueTopic
         */
        this._lastGroupsSet.forEach((groupId) => {
          if (groupId) { // forEach interface is fixed in v4 (may not be undefined)
            this._out.add(QueueTopic(groupId), this._jobDataLens(job))
          }
        })

        cb()
      })
    })
  }

  /**
   * Create new job
   * @param {Object} data
   * @returns {Job}
   */
  add(data: any): void {
    this._source.add(data)
  }

  /**
   * Subscribe for processing using given groupId
   *
   * All subscribers using same groupId will split messages processing
   * @param {ConsumerGroupId} groupId
   * @param {ProcessCallback<Job>} fn
   * @param {number} n
   */
  process(groupId: ConsumerGroupId, fn: ProcessCallback<Job>, n?: number): void {
    /**
     * Register new consumer group so sources can dispatch messages for new group
     */
    this._groups.add(groupId)

    /**
     * Attach handler itself for processing
     */
    this._out.process(QueueTopic(groupId), fn, n)
  }

  /**
   * Removes consumer group for current topic
   * Use this for cleanup, replace process(groupId) with removeGroup(groupId), deploy for a while
   */
  removeGroup(groupId: ConsumerGroupId): this {
    this._groups.remove(groupId)

    return this
  }
}

/**
 * EventBus implementation
 */
export class EventBusImpl<Job> implements EventBus<Job> {
  private _queueMap: Map<QueueTopic, MultiConsumerQueue<Job>> = Map()

  /**
   * @param _buildQueue MultiConsumerQueue builder function
   */
  constructor(private readonly _buildQueue: (t: QueueTopic) => MultiConsumerQueue<Job>) {
  }

  topic(topic: QueueTopic): MultiConsumerQueue<Job> {
    /**
     * Create queue for given topic name and store the reference
     */
    if (!this._queueMap.has(topic)) {
      this._queueMap = this._queueMap.set(topic, this._buildQueue(topic))
    }

    return this._queueMap.get(topic)
  }
}
