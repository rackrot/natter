import {
  encodeEnvelope,
  type Presence,
  presenceSubject,
} from '../../src/protocol'

/** A heartbeat as a worker would write it, with only `fields` set by the test. */
export function presenceBytes(worker: string, fields: Partial<Presence> = {}) {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'presence',
    worker,
    instance: 'iiiiiiiiiiiiiiiiiiiiii',
    host: 'the-host',
    repo: 'the-repo',
    description: 'the description',
    plugin_version: '0.0.0',
    state: 'idle',
    heartbeat_s: 15,
    ...fields,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  return encoded.bytes
}

/** Stores a heartbeat, so the name counts as a worker that has started. */
export const seen = async (
  js: { publish(subject: string, data: Uint8Array): Promise<unknown> },
  worker: string,
  fields: Partial<Presence> = {},
) => {
  await js.publish(presenceSubject(worker), presenceBytes(worker, fields))
}
