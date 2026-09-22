import asyncio
from unittest import mock

from aiohttp import web
from aiohttp.web_protocol import RequestHandler


async def serve(request: web.BaseRequest) -> web.Response:
    return web.Response()


def _make_handler() -> RequestHandler:
    return web.Server(serve)()


async def test_repr() -> None:
    manager = web.Server(serve)
    handler = manager()

    assert "<RequestHandler disconnected>" == repr(handler)

    with mock.patch.object(handler, "transport", autospec=True):
        assert "<RequestHandler connected>" == repr(handler)


async def test_connections() -> None:
    manager = web.Server(serve)
    assert manager.connections == []

    handler = mock.Mock(spec_set=web.RequestHandler)
    handler._task_handler = None
    transport = object()
    manager.connection_made(handler, transport)  # type: ignore[arg-type]
    assert manager.connections == [handler]

    manager.connection_lost(handler, None)
    assert manager.connections == []


async def test_shutdown_no_timeout() -> None:
    manager = web.Server(serve)

    handler = mock.Mock(spec_set=web.RequestHandler)
    handler._task_handler = None
    handler.shutdown = mock.AsyncMock(return_value=mock.Mock())
    transport = mock.Mock()
    manager.connection_made(handler, transport)

    await manager.shutdown()

    manager.connection_lost(handler, None)
    assert manager.connections == []
    handler.shutdown.assert_called_with(None)


async def test_shutdown_timeout() -> None:
    manager = web.Server(serve)

    handler = mock.Mock()
    handler.shutdown = mock.AsyncMock(return_value=mock.Mock())
    transport = mock.Mock()
    manager.connection_made(handler, transport)

    await manager.shutdown(timeout=0.1)

    manager.connection_lost(handler, None)
    assert manager.connections == []
    handler.shutdown.assert_called_with(0.1)


async def test_pause_msg_queue_reading_without_transport() -> None:
    """Pausing with no transport still records the paused state."""
    handler = _make_handler()
    handler.transport = None

    handler._pause_msg_queue_reading()

    assert handler._msg_queue_paused is True


async def test_resume_msg_queue_reading_after_upgrade_skips_reparse() -> None:
    """Resume after an upgrade clears the pause and resumes without reparsing."""
    handler = _make_handler()
    transport = mock.Mock()
    handler.transport = transport
    handler._upgrade = True
    handler._msg_queue_paused = True
    handler._reading_paused = False

    with mock.patch.object(RequestHandler, "data_received") as data_received:
        handler._resume_msg_queue_reading()

    data_received.assert_not_called()
    assert handler._msg_queue_paused is False
    transport.resume_reading.assert_called_once_with()


async def test_resume_msg_queue_reading_without_transport() -> None:
    """Resume clears the pause but does not touch a missing transport."""
    handler = _make_handler()
    handler.transport = None
    handler._upgrade = True  # skip the reparse branch
    handler._msg_queue_paused = True

    handler._resume_msg_queue_reading()

    assert handler._msg_queue_paused is False


async def test_resume_reading_stays_paused_for_msg_queue() -> None:
    """Base resume_reading must not un-pause the transport while queue-paused."""
    handler = _make_handler()
    transport = mock.Mock()
    handler.transport = transport
    # The stream-level pause is what resume_reading() would otherwise undo.
    handler._reading_paused = True
    handler._msg_queue_paused = True

    handler.resume_reading()

    transport.resume_reading.assert_not_called()
    # The stream-level pause is released; the queue pause still owns the
    # transport and _resume_msg_queue_reading() is what lifts it.
    assert handler._reading_paused is False


async def test_pause_msg_queue_reading_ignores_unsupported_transport() -> None:
    """A transport without flow control raising on pause is ignored."""
    handler = _make_handler()
    # Bare asyncio.Transport.pause_reading() raises NotImplementedError.
    handler.transport = asyncio.Transport()

    handler._pause_msg_queue_reading()

    assert handler._msg_queue_paused is True


async def test_resume_msg_queue_reading_ignores_unsupported_transport() -> None:
    """A transport without flow control raising on resume is ignored."""
    handler = _make_handler()
    # Bare asyncio.Transport.resume_reading() raises NotImplementedError.
    handler.transport = asyncio.Transport()
    handler._upgrade = True  # skip the reparse branch
    handler._msg_queue_paused = True

    handler._resume_msg_queue_reading()

    assert handler._msg_queue_paused is False
