import time
from dataclasses import dataclass, field
from typing import Literal, Optional

from .config import DEFAULT_HISTORY_SIZE

RunnerStatus = Literal["connecting", "running", "done", "error"]
ChannelRole = Literal["reader", "writer"]

MAX_LATENCY_SAMPLES = 100


@dataclass
class ChannelStats:
    uri: str
    role: ChannelRole
    message_count: int = 0
    bytes_total: int = 0
    last_message_at: Optional[int] = None
    latencies_ms: list[float] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "uri": self.uri,
            "role": self.role,
            "messageCount": self.message_count,
            "bytesTotal": self.bytes_total,
            "lastMessageAt": self.last_message_at,
            "latenciesMs": self.latencies_ms,
        }


@dataclass
class RunnerStats:
    id: str
    host: str
    uri: str
    connected_at: int
    disconnected_at: Optional[int] = None
    status: RunnerStatus = "connecting"
    grpc_state: str = "IDLE"
    #: Keyed by `<role>:<uri>`; the same URI can appear once per role.
    channels: dict[str, ChannelStats] = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "id": self.id,
            "host": self.host,
            "uri": self.uri,
            "connectedAt": self.connected_at,
            "disconnectedAt": self.disconnected_at,
            "status": self.status,
            "grpcState": self.grpc_state,
            "channels": {key: channel.to_json() for key, channel in self.channels.items()},
        }


class ChannelTracker:
    """Records message statistics for one channel of one runner."""

    def __init__(self, channel: Optional[ChannelStats]):
        self._channel = channel

    def record_message(self, num_bytes: int, latency_ms: Optional[float] = None) -> None:
        channel = self._channel
        if channel is None:
            return
        channel.message_count += 1
        channel.bytes_total += num_bytes
        channel.last_message_at = _now_ms()
        if latency_ms is not None:
            channel.latencies_ms.append(latency_ms)
            if len(channel.latencies_ms) > MAX_LATENCY_SAMPLES:
                channel.latencies_ms.pop(0)


def _now_ms() -> int:
    return int(time.time() * 1000)


class State:
    """Tracks the runners served by this process, for the dashboard and /api/state."""

    def __init__(self, history_size: int = DEFAULT_HISTORY_SIZE):
        self._runners: dict[str, RunnerStats] = {}
        # Most recently disconnected runners, newest first, so the dashboard keeps
        # showing a pipeline run after it finished. -1 means "keep them all".
        self._history: list[RunnerStats] = []
        self._history_size = max(-1, history_size)
        self._next_id = 1

    def register_runner(self, host: str, uri: str) -> str:
        runner_id = str(self._next_id)
        self._next_id += 1
        self._runners[runner_id] = RunnerStats(id=runner_id, host=host, uri=uri, connected_at=_now_ms())
        return runner_id

    def deregister_runner(self, runner_id: str) -> None:
        runner = self._runners.pop(runner_id, None)
        if runner is None:
            return
        runner.disconnected_at = _now_ms()
        if runner.status != "error":
            runner.status = "done"
        self._history.insert(0, runner)
        if self._history_size >= 0:
            del self._history[self._history_size:]

    def set_status(self, runner_id: str, status: RunnerStatus) -> None:
        runner = self._runners.get(runner_id)
        if runner:
            runner.status = status

    def set_grpc_state(self, runner_id: str, grpc_state: str) -> None:
        runner = self._runners.get(runner_id)
        if runner:
            runner.grpc_state = grpc_state

    def mark_error(self, runner_id: str) -> None:
        self.set_status(runner_id, "error")

    def track_channel(self, runner_id: str, uri: str, role: ChannelRole) -> ChannelTracker:
        runner = self._runners.get(runner_id)
        if not runner:
            return ChannelTracker(None)
        # A single channel URI can be read and written inside one runner (a processor feeding
        # another in the same pipeline), so the role is part of the key. Keying on the URI
        # alone would merge both directions into one record: one arbitrary role, counts and
        # bytes summed over both, and reader traffic polluting the writer latencies.
        key = f"{role}:{uri}"
        if key not in runner.channels:
            runner.channels[key] = ChannelStats(uri=uri, role=role)
        return ChannelTracker(runner.channels[key])

    def untrack_channel(self, runner_id: str, uri: str, role: ChannelRole) -> None:
        """Drop a channel's stats entry again, e.g. when its registration is rolled back."""
        runner = self._runners.get(runner_id)
        if runner:
            runner.channels.pop(f"{role}:{uri}", None)

    def snapshot(self) -> list[dict]:
        return [runner.to_json() for runner in (*self._runners.values(), *self._history)]
