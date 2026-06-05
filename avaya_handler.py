"""Avaya RCMS ↔ OpenAI Realtime bridge handler.

Mirrors the pattern of the openai-agents-python Twilio example
(examples/realtime/twilio/twilio_handler.py), adapted for the
Avaya Real-time Contextual Media Streaming (RCMS) protocol.

Avaya RCMS protocol references:
  - Spec PDF: https://github.com/Avaya-Infinity/real-time-contextual-media
  - Sample server: byobot-sample/byobot_server.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
import time
import traceback
import uuid
from datetime import datetime, UTC
from typing import Any

from fastapi import WebSocket

from agents import function_tool
from agents.realtime import (
    RealtimeAgent,
    RealtimePlaybackTracker,
    RealtimeRunner,
    RealtimeSession,
    RealtimeSessionEvent,
)

# ─────────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("avaya_bridge")

# ─────────────────────────────────────────────────────────────────
# Binary frame constants
# ─────────────────────────────────────────────────────────────────

BINARY_MAGIC = 0x4156   # 'AV' — identifies old 52-byte Avaya frames

SOURCE_NONE = 0
SOURCE_TX   = 1   # Caller audio → our server   (egress from Avaya PoV)
SOURCE_RX   = 2   # Bot audio   → caller         (ingress from Avaya PoV)

CODEC_PCMU = 0    # G.711 µ-law  ≡ OpenAI "g711_ulaw"
CODEC_PCMA = 8    # G.711 A-law
CODEC_G722 = 9
CODEC_L16  = 11   # Raw signed 16-bit PCM LE

CODEC_NAME_MAP: dict[str, int] = {
    "PCMU": CODEC_PCMU,
    "PCMA": CODEC_PCMA,
    "G722": CODEC_G722,
    "L16":  CODEC_L16,
}
CODEC_ID_TO_NAME: dict[int, str] = {v: k for k, v in CODEC_NAME_MAP.items()}

# ── Old format: 52-byte header (magic = 0x4156) ──────────────────
_OLD_FMT        = ">HBBHHII16s16sI"
OLD_HEADER_SIZE = struct.calcsize(_OLD_FMT)   # 52
FLAG_OLD_LAST_FRAME = 0x02
FLAG_OLD_SOURCE_TX  = 0x04
assert OLD_HEADER_SIZE == 52

# ── Compact format: 16-byte header ───────────────────────────────
#   B bid(1)  B source(1)  H flags(2)  I seq(4)  Q timestamp_us(8)
#   Payload = everything after the 16-byte header.
_CMP_FMT        = ">BBHIQ"
CMP_HEADER_SIZE = struct.calcsize(_CMP_FMT)   # 16
FLAG_CMP_LAST_FRAME = 0x0001
FLAG_CMP_EXTENSION  = 0x0002
assert CMP_HEADER_SIZE == 16


# ─────────────────────────────────────────────────────────────────
# Binary frame parsing / building
# ─────────────────────────────────────────────────────────────────

def parse_binary_frame(data: bytes) -> dict[str, Any] | None:
    if len(data) < 2:
        return None
    magic = struct.unpack_from(">H", data, 0)[0]

    # Old 52-byte format
    if magic == BINARY_MAGIC:
        if len(data) < OLD_HEADER_SIZE:
            return None
        (_, version, media_type, codec_id, flags, ts_ms, seq,
         sess_b, ep_b, pl_len) = struct.unpack_from(_OLD_FMT, data, 0)
        payload = data[OLD_HEADER_SIZE: OLD_HEADER_SIZE + pl_len]
        return {
            "format":      "old-52",
            "codec_id":    codec_id,
            "is_tx":       bool(flags & FLAG_OLD_SOURCE_TX),
            "is_last":     bool(flags & FLAG_OLD_LAST_FRAME),
            "ts_ms":       ts_ms,
            "seq":         seq,
            "session_id":  _bytes_to_uuid(sess_b),
            "endpoint_id": _bytes_to_uuid(ep_b),
            "payload":     payload,
        }

    # Compact 16-byte format
    if len(data) < CMP_HEADER_SIZE:
        return None
    bid, source, flags, seq, ts_us = struct.unpack_from(_CMP_FMT, data, 0)
    offset = CMP_HEADER_SIZE
    if flags & FLAG_CMP_EXTENSION and len(data) >= offset + 2:
        ext_len = struct.unpack_from(">H", data, offset)[0]
        offset += 2 + ext_len
    payload = data[offset:]
    return {
        "format":   "compact-16",
        "codec_id": None,
        "is_tx":    source == SOURCE_TX,
        "is_last":  bool(flags & FLAG_CMP_LAST_FRAME),
        "ts_us":    ts_us,
        "seq":      seq,
        "bid":      bid,
        "source":   source,
        "payload":  payload,
    }


def build_compact_frame(
    payload: bytes,
    bid: int = 0,
    source: int = SOURCE_RX,
    seq: int = 0,
    is_last: bool = False,
) -> bytes:
    flags  = FLAG_CMP_LAST_FRAME if is_last else 0
    ts_us  = int(time.monotonic() * 1_000_000)
    header = struct.pack(_CMP_FMT, bid, source, flags, seq, ts_us)
    return header + payload


# ─────────────────────────────────────────────────────────────────
# OpenAI Realtime agent
# ─────────────────────────────────────────────────────────────────

@function_tool
def get_weather(city: str) -> str:
    """Get the current weather in a city."""
    return f"The weather in {city} is sunny with mild temperatures."


@function_tool
def get_current_time() -> str:
    """Get the current time."""
    return f"The current time is {datetime.now().strftime('%H:%M:%S')}."


_agent = RealtimeAgent(
    name="Avaya Voice Assistant",
    instructions=(
        "You are a helpful voice assistant on a phone call. "
        "Keep your responses concise and conversational — this is a phone call, "
        "so avoid long lists or complex formatting. "
        "Start every new conversation with a brief, friendly greeting."
    ),
    tools=[get_weather, get_current_time],
)


# ─────────────────────────────────────────────────────────────────
# Main handler
# ─────────────────────────────────────────────────────────────────

class AvayaHandler:
    """Bridges a single Avaya RCMS WebSocket call to OpenAI Realtime API."""

    CHUNK_LENGTH_S = 0.05
    SAMPLE_RATE    = 8_000
    BUFFER_SIZE    = int(SAMPLE_RATE * CHUNK_LENGTH_S)   # 400 bytes = 50 ms PCMU

    # Log binary frame stats every N frames (avoids flooding logs)
    _BINARY_LOG_EVERY = 200

    def __init__(self, websocket: WebSocket) -> None:
        self.ws              = websocket
        self.session: RealtimeSession | None = None
        self.playback_tracker                = RealtimePlaybackTracker()

        self._session_id:  str = ""
        self._endpoint_id: str = ""
        self._bid:         int = 0
        self._codec_id:    int = CODEC_PCMU

        self._seq_out:  int = 0
        self._json_seq: int = 0

        self._audio_buf:  bytearray = bytearray()
        self._last_flush: float     = time.monotonic()

        # Stats
        self._frames_rx:    int = 0   # binary frames received
        self._audio_bytes_rx: int = 0   # raw audio bytes from Avaya
        self._audio_bytes_tx: int = 0   # audio bytes sent to OpenAI
        self._openai_audio_chunks: int = 0   # audio chunks from OpenAI

        self._tasks: list[asyncio.Task] = []

    # ── Lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        client = self.ws.client
        client_addr = f"{client.host}:{client.port}" if client else "unknown"
        log.info("━━━ New WebSocket connection from %s ━━━", client_addr)
        log.debug("Headers: %s", dict(self.ws.headers))
        await self.ws.accept()
        log.info("WebSocket accepted — waiting for Avaya session.start")

    async def wait_until_done(self) -> None:
        log.info("Entering receive loop")
        try:
            while True:
                raw = await self.ws.receive()
                event_type = raw.get("type", "")

                if event_type == "websocket.disconnect":
                    code   = raw.get("code", "?")
                    reason = raw.get("reason", "")
                    log.info("WebSocket disconnected — code=%s reason=%s", code, reason)
                    break

                if "text" in raw and raw["text"]:
                    await self._handle_text(raw["text"])
                elif "bytes" in raw and raw["bytes"]:
                    await self._handle_binary(raw["bytes"])
                else:
                    log.debug("Received empty/unknown frame: type=%s keys=%s",
                              event_type, list(raw.keys()))

        except Exception as exc:
            log.error("Receive loop crashed: %s\n%s", exc, traceback.format_exc())

    async def cleanup(self) -> None:
        log.info(
            "Cleanup — frames_rx=%d  audio_rx=%d B  audio_tx=%d B  openai_chunks=%d",
            self._frames_rx, self._audio_bytes_rx,
            self._audio_bytes_tx, self._openai_audio_chunks,
        )
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("Handler cleaned up")

    # ── JSON / control message handling ─────────────────────────

    async def _handle_text(self, text: str) -> None:
        try:
            msg: dict[str, Any] = json.loads(text)
        except json.JSONDecodeError:
            log.warning("Received non-JSON text frame: %s", text[:500])
            return

        msg_type = msg.get("type", "<no-type>")
        log.info("← RECV [%s]", msg_type)
        log.debug("Full message:\n%s", json.dumps(msg, indent=2))

        if msg_type == "session.start":
            await self._on_session_start(msg)
        elif msg_type == "session.end":
            await self._on_session_end(msg)
        elif msg_type.endswith(".start"):
            await self._on_service_start(msg)
        elif msg_type.endswith(".end"):
            await self._on_service_end(msg)
        else:
            log.warning("Unhandled message type '%s'. Full message:\n%s",
                        msg_type, json.dumps(msg, indent=2))

    async def _on_session_start(self, msg: dict) -> None:
        self._session_id = msg.get("sessionId", str(uuid.uuid4()))
        log.info("session.start — sessionId=%s", self._session_id)

        resp_endpoints = []
        for i, ep in enumerate(msg.get("mediaEndpoints", [])):
            ep_id = ep.get("id", str(uuid.uuid4()))
            if not self._endpoint_id:
                self._endpoint_id = ep_id

            flows      = ep.get("flows", {})
            audio_flow = flows.get("audio", {})
            egress     = audio_flow.get("egress", {})
            offered    = egress.get("codecs", ["PCMU"])
            chosen     = self._pick_codec(offered)

            log.info(
                "  endpoint[%d] id=%s  transport=%s  offered_codecs=%s  chosen=%s",
                i, ep_id, ep.get("transport"), offered, chosen,
            )

            resp_endpoints.append({
                "id":        ep_id,
                "transport": ep.get("transport", "binary"),
                "flows": {
                    "audio": {
                        "egress":  {"codec": chosen, "sampleRate": 8000},
                        "ingress": {"codec": chosen, "sampleRate": 8000},
                    }
                },
            })

        resp: dict[str, Any] = {
            "version":     msg.get("version", "1.0.0"),
            "type":        "session.started",
            "sessionId":   self._session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
        }
        if resp_endpoints:
            resp["mediaEndpoints"] = resp_endpoints

        log.info("→ SEND [session.started]  codec=%s", self._codec_name())
        log.debug("Full response:\n%s", json.dumps(resp, indent=2))
        await self._send_json(resp)

    async def _on_service_start(self, msg: dict) -> None:
        msg_type    = msg.get("type", "")
        session_id  = msg.get("sessionId", self._session_id)
        payload     = msg.get("payload", {})
        endpoint_id = payload.get("endpointId", self._endpoint_id)
        service     = msg.get("service", msg_type.replace(".start", ""))

        if endpoint_id and not self._endpoint_id:
            self._endpoint_id = endpoint_id

        log.info("Service start — type=%s  service=%s  endpoint=%s",
                 msg_type, service, endpoint_id)

        if self.session is None:
            log.info("No OpenAI session yet — initializing now")
            await self._start_openai_session()
        else:
            log.info("OpenAI session already active")

        started_type = msg_type.replace(".start", ".started")
        resp = {
            "version":     msg.get("version", "1.0.0"),
            "type":        started_type,
            "sessionId":   session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        }
        log.info("→ SEND [%s]", started_type)
        log.debug("Full response:\n%s", json.dumps(resp, indent=2))
        await self._send_json(resp)

    async def _on_service_end(self, msg: dict) -> None:
        msg_type    = msg.get("type", "")
        session_id  = msg.get("sessionId", self._session_id)
        payload     = msg.get("payload", {})
        endpoint_id = payload.get("endpointId", self._endpoint_id)
        service     = msg.get("service", msg_type.replace(".end", ""))

        log.info("Service end — type=%s  endpoint=%s", msg_type, endpoint_id)

        ended_type = msg_type.replace(".end", ".ended")
        resp = {
            "version":     msg.get("version", "1.0.0"),
            "type":        ended_type,
            "sessionId":   session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        }
        log.info("→ SEND [%s]", ended_type)
        await self._send_json(resp)

    async def _on_session_end(self, msg: dict) -> None:
        log.info("session.end received — closing")
        resp = {
            "version":     msg.get("version", "1.0.0"),
            "type":        "session.ended",
            "sessionId":   self._session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
        }
        log.info("→ SEND [session.ended]")
        await self._send_json(resp)

    # ── Binary frame handling ────────────────────────────────────

    async def _handle_binary(self, data: bytes) -> None:
        self._frames_rx += 1
        frame = parse_binary_frame(data)

        if frame is None:
            log.warning(
                "Could not parse binary frame #%d (%d bytes) — hex: %s",
                self._frames_rx, len(data), data[:64].hex(),
            )
            return

        # Log the very first frame in detail, then periodically
        if self._frames_rx == 1:
            log.info(
                "First binary frame — format=%s  is_tx=%s  codec_id=%s  "
                "payload=%d B  seq=%s  bid=%s",
                frame["format"], frame["is_tx"], frame.get("codec_id"),
                len(frame.get("payload", b"")), frame.get("seq"), frame.get("bid"),
            )
            log.debug("First frame raw hex: %s", data[:64].hex())
        elif self._frames_rx % self._BINARY_LOG_EVERY == 0:
            log.debug(
                "Binary frames rx=%d  audio_rx=%d B  audio_tx=%d B  "
                "openai_chunks=%d  buf=%d B",
                self._frames_rx, self._audio_bytes_rx,
                self._audio_bytes_tx, self._openai_audio_chunks,
                len(self._audio_buf),
            )

        if not frame["is_tx"]:
            return   # Skip frames we sent (RX / echoed back)

        if frame.get("bid") is not None:
            self._bid = frame["bid"]

        payload = frame.get("payload", b"")
        if not payload:
            return

        self._audio_bytes_rx += len(payload)

        if self.session is not None:
            self._audio_buf.extend(payload)
            if len(self._audio_buf) >= self.BUFFER_SIZE:
                await self._flush_audio()
        else:
            if self._frames_rx % self._BINARY_LOG_EVERY == 0:
                log.debug(
                    "Audio arriving but OpenAI session not started yet "
                    "(frames_rx=%d). Waiting for service start message.",
                    self._frames_rx,
                )

    # ── OpenAI Realtime session ──────────────────────────────────

    async def _start_openai_session(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            log.error("OPENAI_API_KEY is not set — cannot start Realtime session")
            raise ValueError("OPENAI_API_KEY environment variable is required")

        model_name = os.getenv("OPENAI_REALTIME_MODEL", "gpt-4o-realtime-preview")
        log.info("Starting OpenAI Realtime session — model=%s", model_name)

        model_config = {
            "api_key": api_key,
            "initial_model_settings": {
                "model_name":          model_name,
                "input_audio_format":  "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "turn_detection": {
                    "type":               "semantic_vad",
                    "interrupt_response": True,
                    "create_response":    True,
                },
            },
            "playback_tracker": self.playback_tracker,
        }
        log.debug("model_config: %s", json.dumps(
            {k: v for k, v in model_config.items() if k != "api_key"}, indent=2
        ))

        try:
            runner = RealtimeRunner(_agent)
            self.session = await runner.run(model_config=model_config)
            await self.session.enter()
            log.info("OpenAI Realtime session STARTED ✓  model=%s", model_name)
        except Exception as exc:
            log.error("Failed to start OpenAI Realtime session: %s\n%s",
                      exc, traceback.format_exc())
            raise

        self._tasks.append(asyncio.create_task(self._realtime_event_loop()))
        self._tasks.append(asyncio.create_task(self._buffer_flush_loop()))
        log.info("Background tasks started (event loop + buffer flush)")

    async def _realtime_event_loop(self) -> None:
        assert self.session is not None
        log.info("OpenAI event loop running")
        try:
            async for event in self.session:
                await self._handle_realtime_event(event)
        except asyncio.CancelledError:
            log.info("OpenAI event loop cancelled")
        except Exception as exc:
            log.error("OpenAI event loop crashed: %s\n%s", exc, traceback.format_exc())
        log.info("OpenAI event loop exited")

    async def _handle_realtime_event(self, event: RealtimeSessionEvent) -> None:
        if event.type == "audio":
            self._openai_audio_chunks += 1
            if self._openai_audio_chunks == 1:
                log.info("First audio chunk from OpenAI — %d bytes", len(event.audio.data))
            frame = build_compact_frame(
                payload=event.audio.data,
                bid=self._bid,
                source=SOURCE_RX,
                seq=self._seq_out,
                is_last=False,
            )
            self._seq_out += 1
            await self.ws.send_bytes(frame)

        elif event.type == "audio_end":
            log.info("OpenAI audio_end — sending last-frame to Avaya  (chunks=%d)",
                     self._openai_audio_chunks)
            end_frame = build_compact_frame(
                payload=b"",
                bid=self._bid,
                source=SOURCE_RX,
                seq=self._seq_out,
                is_last=True,
            )
            self._seq_out += 1
            await self.ws.send_bytes(end_frame)

        elif event.type == "audio_interrupted":
            log.info("OpenAI audio_interrupted — caller spoke over bot")

        elif event.type == "raw_model_event":
            raw = getattr(event, "data", None) or getattr(event, "event", None)
            if raw:
                etype = raw.get("type", "") if isinstance(raw, dict) else str(raw)[:80]
                log.debug("raw_model_event: %s", etype)

        else:
            log.debug("OpenAI event: type=%s", event.type)

    # ── Audio buffering ──────────────────────────────────────────

    async def _flush_audio(self) -> None:
        if not self._audio_buf or self.session is None:
            return
        data = bytes(self._audio_buf)
        self._audio_buf.clear()
        self._last_flush = time.monotonic()
        self._audio_bytes_tx += len(data)
        await self.session.send_audio(data)

    async def _buffer_flush_loop(self) -> None:
        log.debug("Buffer flush loop started (interval=%.0f ms)", self.CHUNK_LENGTH_S * 1000)
        try:
            while True:
                await asyncio.sleep(self.CHUNK_LENGTH_S)
                now = time.monotonic()
                if self._audio_buf and now - self._last_flush > self.CHUNK_LENGTH_S * 2:
                    await self._flush_audio()
        except asyncio.CancelledError:
            log.debug("Buffer flush loop cancelled")
        except Exception as exc:
            log.error("Buffer flush loop error: %s", exc)

    # ── Utilities ────────────────────────────────────────────────

    def _next_json_seq(self) -> int:
        self._json_seq += 1
        return self._json_seq

    def _codec_name(self) -> str:
        return CODEC_ID_TO_NAME.get(self._codec_id, "PCMU")

    def _pick_codec(self, offered: list[str]) -> str:
        for preferred in ("PCMU", "PCMA"):
            if preferred in offered:
                self._codec_id = CODEC_NAME_MAP[preferred]
                return preferred
        first = offered[0] if offered else "PCMU"
        self._codec_id = CODEC_NAME_MAP.get(first, CODEC_PCMU)
        log.warning("PCMU/PCMA not in offered codecs %s — using %s (may need conversion)",
                    offered, first)
        return first

    async def _send_json(self, payload: dict) -> None:
        await self.ws.send_text(json.dumps(payload))


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def _bytes_to_uuid(b: bytes) -> str:
    try:
        return str(uuid.UUID(bytes=b))
    except Exception:
        return b.hex()


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()
