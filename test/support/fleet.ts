import {
  bootstrapStreams as bootstrap,
  type ManagedStream,
  streamsFor,
} from '../../src/bootstrap'
import { DEFAULT_FLEET } from '../../src/config'
import { fleetScope } from '../../src/protocol'

// The `default` fleet under the names tests use, so most test files only
// import from here. Tests of another fleet build their own `fleetScope`.
export const scope = fleetScope(DEFAULT_FLEET)

export const TASKS_STREAM_NAME = scope.streams.tasks
export const RESULTS_STREAM_NAME = scope.streams.results
export const STATE_STREAM_NAME = scope.streams.state
export const SENT_STREAM_NAME = scope.streams.sent
export const PRESENCE_STREAM_NAME = scope.streams.presence

export const TASK_SUBJECTS = scope.subjects.task
export const RESULT_SUBJECTS = scope.subjects.result
export const STATUS_SUBJECTS = scope.subjects.status
export const PRESENCE_SUBJECTS = scope.subjects.presence
export const SENT_SUBJECTS = scope.subjects.sent

export const {
  taskSubject,
  presenceSubject,
  resultSubject,
  statusSubject,
  sentSubject,
} = scope

export const STREAMS = streamsFor(DEFAULT_FLEET)
export const [
  TASKS_STREAM,
  RESULTS_STREAM,
  STATE_STREAM,
  PRESENCE_STREAM,
  SENT_STREAM,
] = STREAMS as unknown as [
  ManagedStream,
  ManagedStream,
  ManagedStream,
  ManagedStream,
  ManagedStream,
]

export const bootstrapStreams = (
  jsm: Parameters<typeof bootstrap>[0],
  wanted: readonly ManagedStream[] = STREAMS,
) => bootstrap(jsm, wanted)
