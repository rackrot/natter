import { afterAll, beforeAll, expect, test } from 'bun:test'
import { connect, type NatsConnection } from '@nats-io/transport-node'

let server: Bun.Subprocess
let nc: NatsConnection

async function freePort(): Promise<number> {
  const probe = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: { data() {} },
  })
  const { port } = probe
  probe.stop(true)
  return port
}

beforeAll(async () => {
  const port = await freePort()
  server = Bun.spawn(['nats-server', '-a', '127.0.0.1', '-p', String(port)], {
    stdout: 'ignore',
    stderr: 'ignore',
  })
  // Retry until the freshly spawned server accepts connections.
  for (let attempt = 0; ; attempt++) {
    try {
      nc = await connect({ servers: `127.0.0.1:${port}` })
      return
    } catch (err) {
      if (attempt >= 50) throw err
      await Bun.sleep(100)
    }
  }
})

afterAll(async () => {
  await nc?.drain()
  server?.kill()
  await server?.exited
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
