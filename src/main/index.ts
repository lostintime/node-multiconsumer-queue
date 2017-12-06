/*
 * Copyright (c) 2017 by The Kue MultiConsumer Project Developers.
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

export interface Consumer {
  /**
   * Add new Message/Task to queue
   * @param {Object} data
   */
  add(data: any): void
}

export interface Producer<Job> {
  /**
   * Attach queue processing handler with processing parallelism
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn callback
   * @param {number} n parallelism
   */
  process(fn: ProcessCallback<Job>, n?: number): void
}

export interface Queue<Job> extends Producer<Job>, Consumer {

}

export interface NamedConsumer {
  add(name: string, data: any): void
}

export interface NamedProducer<Job> {
  /**
   * Attach queue processing handler for given topic name and parallelism
   * @param {string} name
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn
   * @param {number} n
   */
  process(name: string, fn: ProcessCallback<Job>, n?: number): void
}

/**
 * A Queue with named elements, can be produced/consumed with/by name
 */
export interface NamedQueue<Job> extends NamedProducer<Job>, NamedConsumer {

}

/**
 * Defines Producer than will dispatch jobs to all consumer groups
 * Interface is same with NamedProducer<Job> but name argument means groupId
 */
export interface MultiConsumerProducer<Job> {
  /**
   * Process jobs within group defined by groupId identifier
   * @param {string} groupId consumer group identifier
   * @param {MultiConsumerQueue.ProcessCallback<Job>} fn processing function
   * @param {number} n process parallelism, defaults to 1
   */
  process(groupId: string, fn: ProcessCallback<Job>, n?: number): void
}

/**
 * Queue may be consumed by multiple groups
 */
export interface MultiConsumerQueue<Job> extends Consumer, MultiConsumerProducer<Job> {

}

/**
 * Wraps a NamedQueue for given name, exposes Queue interface
 */
export class NamedQueueWrap<Job> implements Queue<Job> {
  constructor(private readonly _name: string,
              private readonly _out: NamedQueue<Job>) {
  }

  add(data: any): void {
    this._out.add(this._name, data)
  }

  process(fn: ProcessCallback<Job>, n?: number): void {
    this._out.process(this._name, fn, n)
  }
}

/**
 * Passes calls to out NamedQueue by transforming input topic name using give function
 */
export class DynamicallyNamedQueue<Job> implements NamedQueue<Job> {

  constructor(readonly name: (name: string) => string,
              readonly out: NamedQueue<Job>) {
  }

  add(name: string, data: any): void {
    this.out.add(this.name(name), data)
  }

  process(name: string, fn: ProcessCallback<Job>, n?: number): void {
    this.out.process(this.name(name), fn, n)
  }
}

export interface EventBus<Job> {
  topic(name: string): MultiConsumerQueue<Job>
}

/**
 * MultiConsumer queue implementation
 */
export class MultiConsumerQueueImpl<Job> implements MultiConsumerQueue<Job> {
  private _lastGroupsSet: Set<string> = Set()

  constructor(private readonly _source: Queue<Job>,
              private readonly _out: NamedQueue<Job>,
              private readonly _groups: RedisLiveSet<string>,
              private readonly _jobDataLens: (j: Job) => any) {
    this._groups.subscribe((consumers) => {
      this._lastGroupsSet = consumers
    })

    this._source.process((job, cb) => {
      const consumers = this._lastGroupsSet

      consumers.forEach((groupId) => {
        if (groupId) { // forEach interface is fixed in v4 (may not be undefined)
          this._out.add(groupId, this._jobDataLens(job))
        }
      })

      cb()
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
   * Add processing group
   * @param {string} groupId
   * @param {ProcessCallback<Job>} fn
   * @param {number} n
   */
  process(groupId: string, fn: ProcessCallback<Job>, n?: number): void {
    this._groups.add(groupId)
    this._out.process(groupId, fn, n)
  }

  /**
   * Removes consumer group for current topic
   * Use this for cleanup, replace process(groupId) with removeGroup(groupId), deploy for a while
   */
  removeGroup(groupId: string): this {
    this._groups.remove(groupId)
    return this
  }
}

/**
 * EventBus implementation
 */
export class EventBusImpl<Job> implements EventBus<Job> {
  private _queueMap: Map<string, MultiConsumerQueue<Job>> = Map()

  constructor(private readonly _buildQueue: (name: string) => MultiConsumerQueue<Job>) {
  }

  topic(name: string): MultiConsumerQueue<Job> {
    if (!this._queueMap.has(name)) {
      this._queueMap = this._queueMap.set(name, this._buildQueue(name))
    }
    return this._queueMap.get(name)
  }
}
