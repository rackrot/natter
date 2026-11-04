import { afterAll, beforeAll, expect, test } from 'bun:test'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { startNats, type TestNats } from './support/nats'

let nats: TestNats
let nc: NatsConnection

beforeAll(async () => {
  nats = await startNats()
  nc = await connect({ servers: nats.url })
})

afterAll(async () => {
  await nc?.drain()
  await nats?.cleanup()
})

test('transport-node delivers a published message to a subscriber under Bun', async () => {
  expect.assertions(1)
  const sub = nc.subscribe('natter.test')
  nc.publish('natter.test', 'hello')

  for await (const msg of sub) {
    expect(msg.string()).toBe('hello')
    break
  }
})
