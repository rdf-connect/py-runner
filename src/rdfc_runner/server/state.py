import time
from dataclasses import dataclass, field
from typing import Literal, Optional

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
    status: RunnerStatus = "connecting"
    grpc_state: str = "IDLE"
    channels: dict[str, ChannelStats] = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "id": self.id,
            "host": self.host,
            "uri": self.uri,
            "connectedAt": self.connected_at,
            "status": self.status,
            "grpcState": self.grpc_state,
            "channels": {uri: channel.to_json() for uri, channel in self.channels.items()},
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

    def __init__(self):
        self._runners: dict[str, RunnerStats] = {}
        self._next_id = 1

    def register_runner(self, host: str, uri: str) -> str:
        runner_id = str(self._next_id)
        self._next_id += 1
        self._runners[runner_id] = RunnerStats(id=runner_id, host=host, uri=uri, connected_at=_now_ms())
        return runner_id

    def deregister_runner(self, runner_id: str) -> None:
        self._runners.pop(runner_id, None)

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
        if uri not in runner.channels:
            runner.channels[uri] = ChannelStats(uri=uri, role=role)
        return ChannelTracker(runner.channels[uri])

    def snapshot(self) -> list[dict]:
        return [runner.to_json() for runner in self._runners.values()]
