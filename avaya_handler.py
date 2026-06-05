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
import os
import struct
import time
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
# Binary frame constants
# ─────────────────────────────────────────────────────────────────

# Magic bytes that identify the old 52-byte Avaya frame format ('AV')
BINARY_MAGIC = 0x4156

# Source enum (compact format, byte 1 of the header)
SOURCE_NONE = 0
SOURCE_TX   = 1   # Caller audio flowing TO our server  (egress from Avaya PoV)
SOURCE_RX   = 2   # Bot audio flowing FROM our server  (ingress from Avaya PoV)

# Codec IDs (follow RTP standard payload types)
CODEC_PCMU = 0    # G.711 µ-law  → OpenAI "g711_ulaw" — no conversion needed
CODEC_PCMA = 8    # G.711 A-law
CODEC_G722 = 9
CODEC_L16  = 11   # Raw signed 16-bit PCM, little-endian

CODEC_NAME_MAP: dict[str, int] = {
    "PCMU": CODEC_PCMU,
    "PCMA": CODEC_PCMA,
    "G722": CODEC_G722,
    "L16":  CODEC_L16,
}
CODEC_ID_TO_NAME: dict[int, str] = {v: k for k, v in CODEC_NAME_MAP.items()}

# ── Old format: 52-byte header (magic = 0x4156) ──────────────────
#   >HBBHHII16s16sI
#   H  magic        (2)
#   B  version      (1)
#   B  media_type   (1)  0=audio
#   H  codec_id     (2)
#   H  flags        (2)
#   I  timestamp_ms (4)
#   I  sequence     (4)
#   16s session_uuid(16)
#   16s endpoint_uuid(16)
#   I  payload_len  (4)
#   ─────────────────────
#                   52 bytes
_OLD_FMT        = ">HBBHHII16s16sI"
OLD_HEADER_SIZE = struct.calcsize(_OLD_FMT)   # must be 52

FLAG_OLD_LAST_FRAME = 0x02
FLAG_OLD_SOURCE_TX  = 0x04   # Set when frame carries caller audio

assert OLD_HEADER_SIZE == 52, f"Old header size mismatch: {OLD_HEADER_SIZE}"

# ── Compact format: 16-byte header (no magic) ────────────────────
#   >BBHIQ
#   B  bid           (1)   bus-id assigned by Avaya
#   B  source        (1)   SOURCE_TX=1, SOURCE_RX=2
#   H  flags         (2)
#   I  sequence      (4)
#   Q  timestamp_us  (8)   microseconds (NTP-based, 64-bit)
#   ─────────────────────
#                   16 bytes
#   Payload = everything after the 16-byte header.
#   If FLAG_CMP_EXTENSION is set, extension data precedes the payload:
#     H ext_length (2) + ext_length bytes of extension data.
_CMP_FMT        = ">BBHIQ"
CMP_HEADER_SIZE = struct.calcsize(_CMP_FMT)   # must be 16

FLAG_CMP_LAST_FRAME = 0x0001
FLAG_CMP_EXTENSION  = 0x0002
FLAG_CMP_CODEC_CHG  = 0x0004

assert CMP_HEADER_SIZE == 16, f"Compact header size mismatch: {CMP_HEADER_SIZE}"


# ─────────────────────────────────────────────────────────────────
# Binary frame parsing / building
# ─────────────────────────────────────────────────────────────────

def parse_binary_frame(data: bytes) -> dict[str, Any] | None:
    """Parse an Avaya RCMS binary frame.

    Returns a dict with at least:
      - is_tx (bool):  True when this frame carries caller audio
      - is_last (bool): True when this is the last frame of a burst
      - payload (bytes): raw audio bytes (PCMU / g711_ulaw if negotiated)
    Returns None if the data cannot be parsed.
    """
    if len(data) < 2:
        return None

    magic = struct.unpack_from(">H", data, 0)[0]

    # ── Old 52-byte format ──────────────────────────────────────
    if magic == BINARY_MAGIC:
        if len(data) < OLD_HEADER_SIZE:
            return None
        (_, version, media_type, codec_id, flags, ts_ms, seq,
         sess_b, ep_b, pl_len) = struct.unpack_from(_OLD_FMT, data, 0)
        payload = data[OLD_HEADER_SIZE: OLD_HEADER_SIZE + pl_len]
        return {
            "format":      "old",
            "codec_id":    codec_id,
            "is_tx":       bool(flags & FLAG_OLD_SOURCE_TX),
            "is_last":     bool(flags & FLAG_OLD_LAST_FRAME),
            "ts_ms":       ts_ms,
            "seq":         seq,
            "session_id":  _bytes_to_uuid(sess_b),
            "endpoint_id": _bytes_to_uuid(ep_b),
            "payload":     payload,
        }

    # ── Compact 16-byte format ──────────────────────────────────
    if len(data) < CMP_HEADER_SIZE:
        return None
    bid, source, flags, seq, ts_us = struct.unpack_from(_CMP_FMT, data, 0)

    # Skip optional extension block
    offset = CMP_HEADER_SIZE
    if flags & FLAG_CMP_EXTENSION and len(data) >= offset + 2:
        ext_len = struct.unpack_from(">H", data, offset)[0]
        offset += 2 + ext_len

    payload = data[offset:]
    return {
        "format":   "compact",
        "codec_id": None,         # codec established during session negotiation
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
    """Build a compact 16-byte Avaya RCMS binary frame for bot audio (RX/ingress)."""
    flags  = FLAG_CMP_LAST_FRAME if is_last else 0
    ts_us  = int(time.monotonic() * 1_000_000)   # relative microseconds
    header = struct.pack(_CMP_FMT, bid, source, flags, seq, ts_us)
    return header + payload


# ─────────────────────────────────────────────────────────────────
# OpenAI Realtime agent definition
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
# Main handler class
# ─────────────────────────────────────────────────────────────────

class AvayaHandler:
    """Bridges a single Avaya RCMS WebSocket call to OpenAI Realtime API."""

    # Audio chunking — match OpenAI's preferred 50 ms PCMU/g711_ulaw chunks
    CHUNK_LENGTH_S = 0.05          # 50 ms
    SAMPLE_RATE    = 8_000         # PCMU / g711_ulaw at 8 kHz
    BUFFER_SIZE    = int(SAMPLE_RATE * CHUNK_LENGTH_S)  # 400 bytes

    def __init__(self, websocket: WebSocket) -> None:
        self.ws              = websocket
        self.session: RealtimeSession | None  = None
        self.playback_tracker                 = RealtimePlaybackTracker()

        # Avaya session metadata
        self._session_id:   str = ""
        self._endpoint_id:  str = ""
        self._bid:          int = 0        # bus-id from compact frames
        self._codec_id:     int = CODEC_PCMU

        # Outbound sequence counter (our frames → Avaya)
        self._seq_out: int = 0

        # Sequence counter for JSON control messages
        self._json_seq: int = 0

        # Inbound audio buffer (caller → OpenAI)
        self._audio_buf:   bytearray = bytearray()
        self._last_flush:  float     = time.monotonic()

        # Background asyncio tasks
        self._tasks: list[asyncio.Task] = []

    # ── Lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        """Accept the WebSocket connection."""
        await self.ws.accept()
        print("[Avaya] WebSocket connection accepted")

    async def wait_until_done(self) -> None:
        """Drive the session until the WebSocket closes."""
        try:
            while True:
                raw = await self.ws.receive()
                event_type = raw.get("type", "")

                if event_type == "websocket.disconnect":
                    print("[Avaya] Client disconnected")
                    break

                if "text" in raw and raw["text"]:
                    await self._handle_text(raw["text"])
                elif "bytes" in raw and raw["bytes"]:
                    await self._handle_binary(raw["bytes"])

        except Exception as exc:
            print(f"[Avaya] Receive loop error: {exc}")

    async def cleanup(self) -> None:
        """Cancel all background tasks."""
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        print("[Avaya] Handler cleaned up")

    # ── JSON / control message handling ─────────────────────────

    async def _handle_text(self, text: str) -> None:
        try:
            msg: dict[str, Any] = json.loads(text)
        except json.JSONDecodeError:
            print(f"[Avaya] Non-JSON text frame: {text[:200]}")
            return

        msg_type = msg.get("type", "")
        print(f"[Avaya] ← {msg_type}")

        if msg_type == "session.start":
            await self._on_session_start(msg)
        elif msg_type == "session.end":
            await self._on_session_end(msg)
        elif msg_type.endswith(".start"):
            # Generic service start — e.g. echo.start, streaming.start, bot.start
            await self._on_service_start(msg)
        elif msg_type.endswith(".end"):
            await self._on_service_end(msg)
        else:
            print(f"[Avaya] Unhandled control message:\n{json.dumps(msg, indent=2)}")

    async def _on_session_start(self, msg: dict) -> None:
        self._session_id = msg.get("sessionId", str(uuid.uuid4()))

        # Negotiate codec from offered mediaEndpoints
        resp_endpoints = []
        for ep in msg.get("mediaEndpoints", []):
            ep_id = ep.get("id", str(uuid.uuid4()))
            if not self._endpoint_id:
                self._endpoint_id = ep_id

            # Parse offered codecs (prefer PCMU for direct OpenAI compatibility)
            flows        = ep.get("flows", {})
            audio_flow   = flows.get("audio", {})
            egress       = audio_flow.get("egress", {})
            offered      = egress.get("codecs", ["PCMU"])
            chosen_codec = self._pick_codec(offered)

            resp_endpoints.append({
                "id":        ep_id,
                "transport": ep.get("transport", "binary"),
                "flows": {
                    "audio": {
                        "egress": {
                            "codec":      chosen_codec,
                            "sampleRate": 8000,
                        },
                        "ingress": {
                            "codec":      chosen_codec,
                            "sampleRate": 8000,
                        },
                    }
                },
            })

        resp: dict[str, Any] = {
            "version":      msg.get("version", "1.0.0"),
            "type":         "session.started",
            "sessionId":    self._session_id,
            "sequenceNum":  self._next_json_seq(),
            "timestamp":    _iso_now(),
        }
        if resp_endpoints:
            resp["mediaEndpoints"] = resp_endpoints

        await self._send_json(resp)
        print(
            f"[Avaya] → session.started  "
            f"session={self._session_id}  codec={self._codec_name()}"
        )

    async def _on_service_start(self, msg: dict) -> None:
        """Handle service-specific start (e.g. echo.start, bot.start, streaming.start)."""
        msg_type    = msg.get("type", "")
        session_id  = msg.get("sessionId", self._session_id)
        payload     = msg.get("payload", {})
        endpoint_id = payload.get("endpointId", self._endpoint_id)
        service     = msg.get("service", msg_type.replace(".start", ""))

        if endpoint_id and not self._endpoint_id:
            self._endpoint_id = endpoint_id

        print(f"[Avaya] Service start — type={msg_type}  endpoint={endpoint_id}")

        if self.session is None:
            await self._start_openai_session()

        started_type = msg_type.replace(".start", ".started")
        await self._send_json({
            "version":     msg.get("version", "1.0.0"),
            "type":        started_type,
            "sessionId":   session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        })
        print(f"[Avaya] → {started_type}")

    async def _on_service_end(self, msg: dict) -> None:
        msg_type    = msg.get("type", "")
        session_id  = msg.get("sessionId", self._session_id)
        payload     = msg.get("payload", {})
        endpoint_id = payload.get("endpointId", self._endpoint_id)
        service     = msg.get("service", msg_type.replace(".end", ""))

        print(f"[Avaya] Service end — type={msg_type}")

        ended_type = msg_type.replace(".end", ".ended")
        await self._send_json({
            "version":     msg.get("version", "1.0.0"),
            "type":        ended_type,
            "sessionId":   session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
            "service":     service,
            "payload":     {"endpointId": endpoint_id},
        })
        print(f"[Avaya] → {ended_type}")

    async def _on_session_end(self, msg: dict) -> None:
        await self._send_json({
            "version":     msg.get("version", "1.0.0"),
            "type":        "session.ended",
            "sessionId":   self._session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
        })
        print("[Avaya] → session.ended")

    # ── Binary frame handling ────────────────────────────────────

    async def _handle_binary(self, data: bytes) -> None:
        frame = parse_binary_frame(data)
        if frame is None:
            print(
                f"[Avaya] Could not parse binary frame "
                f"({len(data)} bytes) — first 32 bytes: {data[:32].hex()}"
            )
            return

        # Skip frames that we sent (echoed back or RX confirmation)
        if not frame["is_tx"]:
            return

        # Track the bus-id for outbound frames
        if frame.get("bid") is not None:
            self._bid = frame["bid"]

        payload = frame.get("payload", b"")
        if payload and self.session is not None:
            self._audio_buf.extend(payload)
            if len(self._audio_buf) >= self.BUFFER_SIZE:
                await self._flush_audio()

    # ── OpenAI Realtime session ──────────────────────────────────

    async def _start_openai_session(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")

        model_name = os.getenv("OPENAI_REALTIME_MODEL", "gpt-4o-realtime-preview")

        runner = RealtimeRunner(_agent)
        self.session = await runner.run(
            model_config={
                "api_key": api_key,
                "initial_model_settings": {
                    "model_name":          model_name,
                    # PCMU (g711_ulaw) requires no codec conversion — Avaya ↔ OpenAI
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
        )
        await self.session.enter()
        print(f"[OpenAI] Realtime session started  model={model_name}")

        self._tasks.append(asyncio.create_task(self._realtime_event_loop()))
        self._tasks.append(asyncio.create_task(self._buffer_flush_loop()))

    async def _realtime_event_loop(self) -> None:
        assert self.session is not None
        try:
            async for event in self.session:
                await self._handle_realtime_event(event)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[OpenAI] Event loop error: {exc}")

    async def _handle_realtime_event(self, event: RealtimeSessionEvent) -> None:
        if event.type == "audio":
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
            # Signal end of bot utterance with the last-frame flag
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
            # Caller interrupted — stop sending bot audio
            print("[OpenAI] Caller interrupted bot audio")

        elif event.type == "raw_model_event":
            pass   # ignore low-level model events

    # ── Audio buffering ──────────────────────────────────────────

    async def _flush_audio(self) -> None:
        if not self._audio_buf or self.session is None:
            return
        data = bytes(self._audio_buf)
        self._audio_buf.clear()
        self._last_flush = time.monotonic()
        await self.session.send_audio(data)

    async def _buffer_flush_loop(self) -> None:
        """Periodically flush the audio buffer so stale data is not held back."""
        try:
            while True:
                await asyncio.sleep(self.CHUNK_LENGTH_S)
                now = time.monotonic()
                if self._audio_buf and now - self._last_flush > self.CHUNK_LENGTH_S * 2:
                    await self._flush_audio()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[Avaya] Buffer flush loop error: {exc}")

    # ── Utilities ────────────────────────────────────────────────

    def _next_json_seq(self) -> int:
        self._json_seq += 1
        return self._json_seq

    def _codec_name(self) -> str:
        return CODEC_ID_TO_NAME.get(self._codec_id, "PCMU")

    def _pick_codec(self, offered: list[str]) -> str:
        """Select codec from offered list, preferring PCMU (OpenAI-compatible)."""
        for preferred in ("PCMU", "PCMA"):
            if preferred in offered:
                self._codec_id = CODEC_NAME_MAP[preferred]
                return preferred
        # Fallback to first offered codec
        first = offered[0] if offered else "PCMU"
        self._codec_id = CODEC_NAME_MAP.get(first, CODEC_PCMU)
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
