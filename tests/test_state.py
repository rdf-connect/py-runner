from rdfc_runner.server.state import State


def uris(snapshot: list[dict]) -> list[str]:
    return [runner["uri"] for runner in snapshot]


def test_live_runner_has_no_disconnected_at():
    state = State()
    state.register_runner("127.0.0.1", "urn:runner")

    [runner] = state.snapshot()
    assert runner["status"] == "connecting"
    assert runner["disconnectedAt"] is None


def test_deregistered_runner_is_kept_as_done_history():
    state = State()
    runner_id = state.register_runner("127.0.0.1", "urn:runner")
    state.set_status(runner_id, "running")

    state.deregister_runner(runner_id)

    [runner] = state.snapshot()
    assert runner["uri"] == "urn:runner"
    assert runner["status"] == "done"
    assert runner["disconnectedAt"] >= runner["connectedAt"]


def test_failed_runner_keeps_error_status():
    state = State()
    runner_id = state.register_runner("127.0.0.1", "urn:runner")
    state.mark_error(runner_id)

    state.deregister_runner(runner_id)

    [runner] = state.snapshot()
    assert runner["status"] == "error"
    assert runner["disconnectedAt"] is not None


def test_live_runners_precede_history_newest_first():
    state = State()
    for uri in ("urn:one", "urn:two"):
        state.deregister_runner(state.register_runner("127.0.0.1", uri))
    state.register_runner("127.0.0.1", "urn:live")

    assert uris(state.snapshot()) == ["urn:live", "urn:two", "urn:one"]


def test_history_is_trimmed_to_its_size():
    state = State(history_size=2)
    for uri in ("urn:one", "urn:two", "urn:three"):
        state.deregister_runner(state.register_runner("127.0.0.1", uri))

    assert uris(state.snapshot()) == ["urn:three", "urn:two"]


def test_history_size_minus_one_keeps_everything():
    state = State(history_size=-1)
    for index in range(10):
        state.deregister_runner(state.register_runner("127.0.0.1", f"urn:{index}"))

    assert len(state.snapshot()) == 10


def test_history_size_zero_keeps_nothing():
    state = State(history_size=0)
    state.deregister_runner(state.register_runner("127.0.0.1", "urn:runner"))

    assert state.snapshot() == []


def test_deregistering_an_unknown_runner_is_a_no_op():
    state = State()

    state.deregister_runner("nope")

    assert state.snapshot() == []


def test_channels_of_a_finished_runner_survive():
    state = State()
    runner_id = state.register_runner("127.0.0.1", "urn:runner")
    state.track_channel(runner_id, "urn:channel", "writer").record_message(10, latency_ms=1.5)

    state.deregister_runner(runner_id)

    [runner] = state.snapshot()
    assert runner["channels"]["urn:channel"]["messageCount"] == 1
