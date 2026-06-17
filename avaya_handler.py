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
import base64
import json
import logging
import os
import struct
import time
import traceback
import uuid
from datetime import datetime, UTC
from typing import Any

import websockets
from fastapi import WebSocket

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
#   H flags(2)  B bid(1)  B source(1)  I seq(4)  Q timestamp_us(8)
#   Payload = everything after the 16-byte header.
_CMP_FMT        = ">HBBIQ"
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
    flags, bid, source, seq, ts_us = struct.unpack_from(_CMP_FMT, data, 0)
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
    source: int = SOURCE_NONE,
    seq: int = 0,
    ts_us: int = 0,
    is_last: bool = False,
) -> bytes:
    flags  = FLAG_CMP_LAST_FRAME if is_last else 0
    header = struct.pack(_CMP_FMT, flags, bid, source, seq, ts_us)
    return header + payload


# ─────────────────────────────────────────────────────────────────
# Ingress streamer — chunks and paces bot audio → Avaya
# Ported from byobot_server.py IngressStreamer (byobot_server.py:751-1264)
# ─────────────────────────────────────────────────────────────────

class _IngressStreamer:
    """Accepts raw PCMU blobs from OpenAI, chunks them to 20 ms slices,
    and paces them to the Avaya WebSocket at real-time rate."""

    CHUNK_MS   = 100
    SAMPLE_RATE = 8_000
    CHUNK_SIZE  = (SAMPLE_RATE * CHUNK_MS) // 1000   # 160 bytes @ 8 kHz PCMU
    CHUNK_US   = CHUNK_MS * 1_000                    # 20 000 µs per chunk

    def __init__(self, ws: WebSocket, bid: int) -> None:
        self._ws       = ws
        self._bid      = bid
        self._buf:  bytearray = bytearray()
        self._seq:  int = 0
        self._ts_us: int = 0           # set to epoch µs on first send
        self._is_last_pending = False  # mark final chunk when audio_end arrives
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._task:  asyncio.Task | None = None
        self._frames_tx = 0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._streaming_loop())

    async def queue_audio(self, data: bytes) -> None:
        await self._queue.put(data)
        self.start()

    async def mark_last(self) -> None:
        """Signal that the current audio response has ended."""
        await self._queue.put(None)   # sentinel

    async def barge_in(self) -> None:
        """Cancel current playback and send a zero-payload LAST frame."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # drain queue
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._buf.clear()
        # single empty LAST frame to tell Avaya to stop
        ts = self._ts_us if self._ts_us else int(time.time() * 1_000_000)
        frame = build_compact_frame(
            payload=b"",
            bid=self._bid,
            source=SOURCE_NONE,
            seq=self._seq,
            ts_us=ts,
            is_last=True,
        )
        self._seq += 1
        await self._ws.send_bytes(frame)
        log.info("← barge-in: sent empty LAST frame  bid=%d seq=%d", self._bid, self._seq - 1)
        self._task = None

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _streaming_loop(self) -> None:
        pacing_start: float | None = None
        chunks_sent = 0

        try:
            while True:
                # Fill buffer from queue until we have at least one chunk
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    # Flush any trailing partial chunk on idle timeout
                    if self._buf:
                        await self._send_chunk(bytes(self._buf), is_last=True,
                                               chunks_sent=chunks_sent)
                    break

                if item is None:
                    # End-of-response sentinel: flush remaining buffer as LAST
                    if self._buf:
                        await self._send_chunk(bytes(self._buf), is_last=True,
                                               chunks_sent=chunks_sent)
                        chunks_sent += 1
                    else:
                        # Empty buf: send zero-payload LAST frame
                        ts = self._ts_us if self._ts_us else int(time.time() * 1_000_000)
                        frame = build_compact_frame(
                            payload=b"", bid=self._bid, source=SOURCE_NONE,
                            seq=self._seq, ts_us=ts, is_last=True,
                        )
                        self._seq += 1
                        await self._ws.send_bytes(frame)
                    self._buf.clear()
                    pacing_start = None
                    chunks_sent = 0
                    continue

                self._buf.extend(item)

                # Drain full chunks from buffer
                while len(self._buf) >= self.CHUNK_SIZE:
                    chunk = bytes(self._buf[:self.CHUNK_SIZE])
                    del self._buf[:self.CHUNK_SIZE]

                    if pacing_start is None:
                        pacing_start = time.monotonic()
                        self._ts_us = int(time.time() * 1_000_000)

                    await self._send_chunk(chunk, is_last=False,
                                           chunks_sent=chunks_sent)
                    chunks_sent += 1

                    # Pace: sleep until the next chunk boundary
                    target = pacing_start + chunks_sent * (self.CHUNK_MS / 1000.0)
                    sleep_s = target - time.monotonic()
                    if sleep_s > 0:
                        await asyncio.sleep(sleep_s)
                    elif sleep_s < -(self.CHUNK_MS / 1000.0):
                        log.warning("Ingress pacing falling behind by %.1f ms", -sleep_s * 1000)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("_IngressStreamer loop crashed: %s", exc)

    async def _send_chunk(self, chunk: bytes, is_last: bool, chunks_sent: int) -> None:
        ts = self._ts_us + chunks_sent * self.CHUNK_US
        frame = build_compact_frame(
            payload=chunk,
            bid=self._bid,
            source=SOURCE_NONE,
            seq=self._seq,
            ts_us=ts,
            is_last=is_last,
        )
        self._seq += 1
        self._frames_tx += 1
        await self._ws.send_bytes(frame)
        if self._frames_tx == 1:
            log.info("→ first send  bid=%d source=0 seq=%d ts_us=%d payload=%d B",
                     self._bid, self._seq - 1, ts, len(chunk))
        elif self._frames_tx % 50 == 0:
            log.debug("→ frames_tx=%d bid=%d seq=%d payload=%d B last=%s",
                      self._frames_tx, self._bid, self._seq - 1, len(chunk), is_last)



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
        self._openai_ws: websockets.ClientConnection | None = None

        self._session_id:  str = ""
        self._endpoint_id: str = ""
        self._codec_id:    int = CODEC_PCMU

        # ingress_bid per endpoint — assigned during session.start (monotonic counter
        # shared with egress bids, matching byobot_server.py:1790-1853)
        self._ingress_bid_by_endpoint: dict[str, int] = {}

        self._json_seq: int = 0

        self._audio_buf:  bytearray = bytearray()
        self._last_flush: float     = time.monotonic()

        self._streamer: _IngressStreamer | None = None

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
        if self._streamer:
            await self._streamer.stop()
        if self._openai_ws is not None:
            try:
                await self._openai_ws.close()
            except Exception:
                pass
        log.info(
            "Cleanup — frames_rx=%d  audio_rx=%d B  audio_tx=%d B  openai_chunks=%d  frames_tx=%d",
            self._frames_rx, self._audio_bytes_rx,
            self._audio_bytes_tx, self._openai_audio_chunks,
            self._streamer._frames_tx if self._streamer else 0,
        )
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("Handler cleaned up")

    # ── JSON / control message handling ─────────────────────────

    async def _handle_text(self, text: str) -> None:
        # Strip UTF-8 BOM if present (some Avaya versions add it)
        text = text.lstrip("﻿").strip()

        # Avaya sends multiple JSON objects concatenated in one WS frame
        # (e.g. session.start + bot.start together). Use raw_decode to handle each one.
        # strict=False additionally allows literal control chars inside JSON strings.
        decoder = json.JSONDecoder(strict=False)
        pos = 0
        while pos < len(text):
            try:
                msg, pos = decoder.raw_decode(text, pos)
            except json.JSONDecodeError as exc:
                log.error(
                    "JSON parse error at offset %d (absolute pos %d) — %s\n"
                    "Remaining snippet: %s",
                    exc.pos, pos + exc.pos, exc.msg, text[pos: pos + 200],
                )
                break

            # skip whitespace between objects
            while pos < len(text) and text[pos] in " \t\n\r":
                pos += 1

            msg_type = msg.get("type", "<no-type>")
            log.info("← RECV [%s]", msg_type)
            #log.debug("Full message:\n%s", json.dumps(msg, indent=2))

            if msg_type == "session.start":
                await self._on_session_start(msg)
            elif msg_type == "media":
                await self._on_media(msg)
            elif msg_type == "session.ping":
                await self._on_session_ping(msg)
            elif msg_type == "session.end":
                await self._on_session_end(msg)
            elif msg_type.endswith(".start"):
                await self._on_service_start(msg)
            elif msg_type.endswith(".end"):
                await self._on_service_end(msg)
            else:
                log.warning("Unhandled message type '%s'. Full message:\n%s",
                            msg_type, json.dumps(msg))

    async def _on_session_start(self, msg: dict) -> None:
        self._session_id = msg.get("sessionId", str(uuid.uuid4()))
        log.info("session.start — sessionId=%s", self._session_id)

        # mediaTransports and mediaEndpoints live inside msg["payload"]
        payload_body = msg.get("payload", {})
        log.info("UMA session payload:\n%s", json.dumps(payload_body))
        log.debug("session.start payload keys: %s", list(payload_body.keys()))

        # ── Pick transport encoding and codec from mediaTransports ──
        transports = payload_body.get("mediaTransports", [])
        selected_transport_type = "avaya-wss"
        selected_encoding       = "binary"
        selected_codec_entry    = ["audio", "PCMU", 8000, 1]

        if transports:
            t = transports[0]
            selected_transport_type = t.get("type", "avaya-wss")
            encodings = t.get("transportEncodings", ["binary"])
            selected_encoding = "binary" if "binary" in encodings else encodings[0]
            codecs = t.get("mediaCodecs", [["audio", "PCMU", 8000, 1]])
            self._pick_codec([c[1] if isinstance(c, list) else c for c in codecs])
            for c in codecs:
                name = c[1] if isinstance(c, list) else c
                if name in ("PCMU", "PCMA"):
                    selected_codec_entry = c if isinstance(c, list) else ["audio", name, 8000, 1]
                    break
            else:
                selected_codec_entry = codecs[0] if codecs else selected_codec_entry
        else:
            log.warning("No mediaTransports in session.start payload — using PCMU defaults")

        log.info(
            "  transport=%s  encoding=%s  codec=%s",
            selected_transport_type, selected_encoding, self._codec_name(),
        )

        # ── Read bid assignments directly from mediaEndpoints (Avaya provides them) ──
        # Avaya sends: egress.bid (caller→bot) and ingress.bid (bot→caller)
        for i, ep in enumerate(payload_body.get("mediaEndpoints", [])):
            ep_id   = ep.get("endpointId", ep.get("id", str(uuid.uuid4())))
            flows   = ep.get("flows", {})
            audio   = flows.get("audio", {})
            egress  = audio.get("egress", {})
            ingress = audio.get("ingress", {})

            if not self._endpoint_id:
                self._endpoint_id = ep_id

            egress_bid = egress.get("bid")
            ingress_bid = ingress.get("bid")
            ingress_target = ingress.get("target", [])
            supports_ingress = bool(
                ingress_target and ingress_target != ["none"] and ingress_bid is not None
            )

            log.info(
                "  endpoint[%d] id=%s  egress_bid=%s  sources=%s  ingress_bid=%s  target=%s",
                i, ep_id, egress_bid, egress.get("sources", []),
                ingress_bid, ingress_target,
            )

            if supports_ingress:
                self._ingress_bid_by_endpoint[ep_id] = ingress_bid

        log.info("  ingress_bids=%s", self._ingress_bid_by_endpoint)

        # ── Reply with schema-compliant session.started (RCMS spec §session.started) ──
        resp: dict[str, Any] = {
            "version":     msg.get("version", "1.0.0"),
            "type":        "session.started",
            "sessionId":   self._session_id,
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
            "payload": {
                "services": list(payload_body.get("services", [])),
                "mediaTransport": {
                    "type":              selected_transport_type,
                    "preferredPTimeMs":  _IngressStreamer.CHUNK_MS,
                    "dtx":               "auto",
                    "mediaCodecs":       [selected_codec_entry],
                    "transportEncoding": selected_encoding,
                },
            },
        }

        log.info("→ SEND [session.started]  codec=%s  encoding=%s",
                 self._codec_name(), selected_encoding)
        log.debug("Full response:\n%s", json.dumps(resp, indent=2))
        await self._send_json(resp)

    async def _on_session_ping(self, msg: dict) -> None:
        resp = {
            "version":     msg.get("version", "1.0.0"),
            "type":        "session.pong",
            "sessionId":   self._session_id or msg.get("sessionId", ""),
            "sequenceNum": self._next_json_seq(),
            "timestamp":   _iso_now(),
        }
        log.info("→ SEND [session.pong]")
        await self._send_json(resp)

    async def _on_service_start(self, msg: dict) -> None:
        msg_type    = msg.get("type", "")
        session_id  = msg.get("sessionId", self._session_id)
        payload     = msg.get("payload", {})
        log.info("UMA session payload (type=%s): %s", msg_type, payload)
        endpoint_id = payload.get("endpointId", self._endpoint_id)
        service     = msg.get("service", msg_type.replace(".start", ""))

        if endpoint_id and not self._endpoint_id:
            self._endpoint_id = endpoint_id

        log.info("Service start — type=%s  service=%s  endpoint=%s",
                 msg_type, service, endpoint_id)

        if self._openai_ws is None:
            log.info("No OpenAI session yet — initializing now")
            context = payload.get("context", {})
            await self._start_openai_session(context)
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

    async def _on_media(self, msg: dict) -> None:
        # Only forward caller→bot audio (src="tx"); ignore bot→caller echoes (src="rx")
        src = msg.get("src", "")
        if src != "tx":
            return
        audio_b64 = msg.get("audio", "")
        if not audio_b64:
            return
        audio_bytes = base64.b64decode(audio_b64)
        self._audio_bytes_rx += len(audio_bytes)
        self._frames_rx += 1
        if self._frames_rx == 1:
            log.info("Primera media TX recibida — %d bytes b64-decoded", len(audio_bytes))
        if self._openai_ws is not None:
            self._audio_buf.extend(audio_bytes)
            if len(self._audio_buf) >= self.BUFFER_SIZE:
                await self._flush_audio()
        else:
            if self._frames_rx % self._BINARY_LOG_EVERY == 0:
                log.warning(
                    "media TX llegando pero OpenAI no conectado aún (count=%d, bytes_rx=%d)",
                    self._frames_rx, self._audio_bytes_rx,
                )

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

        payload = frame.get("payload", b"")
        if not payload:
            return

        self._audio_bytes_rx += len(payload)

        if self._openai_ws is not None:
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

    async def _start_openai_session(self, context: dict = None) -> None:
        if context is None:
            context = {}
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            log.error("OPENAI_API_KEY is not set — cannot start Realtime session")
            raise ValueError("OPENAI_API_KEY environment variable is required")

        model_name = os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime-mini")
        url = f"wss://api.openai.com/v1/realtime?model={model_name}"
        headers = {
            "Authorization": f"Bearer {api_key}",
             "OpenAI-Safety-Identifier": "hashed-user-id"
        }

        log.info("Conectando a OpenAI Realtime — url=%s  model=%s", url, model_name)
        try:
            self._openai_ws = await websockets.connect(url, additional_headers=headers)
        except Exception as exc:
            log.error("Failed to connect to OpenAI Realtime WebSocket: %s\n%s",
                      exc, traceback.format_exc())
            raise
        log.info("WebSocket OpenAI ABIERTO ✓  model=%s", model_name)

        # g711_ulaw = PCMU (8 kHz, µ-law), g711_alaw = PCMA
        audio_fmt = "g711_ulaw" if self._codec_id == CODEC_PCMU else "g711_alaw"
        eagerness = context.get("OPENAI_VAD_EAGERNESS", os.getenv("OPENAI_VAD_EAGERNESS", "auto"))
        instructions = context.get("OPENAI_INSTRUCTIONS", os.getenv("OPENAI_INSTRUCTIONS", "Be extra nice today!"))
        welcome_instructions = context.get(
            "OPENAI_WELCOME_INSTRUCTIONS",
            os.getenv(
                "OPENAI_WELCOME_INSTRUCTIONS",
                "Inicia la conversación inmediatamente mencionando \"Bienvenido a AVAYA ¿Cómo te puedo ayudar? el día de hoy.\"",
            ),
        )
        open_ai_voice = context.get("OPENAI_VOICE", os.getenv("OPENAI_VOICE", "alloy"))  # alloy, echo, fable, onyx, nova, shimmer
        session_cfg = {
        "type": "realtime",
        "model": "gpt-realtime-mini",
        "output_modalities": ["audio"],
        "tools": [
            {
                "type": "function",
                "name": "transferir_a_agente",
                "description": "Llama a esta función inmediatamente cuando el usuario pida hablar con un humano, agente, operador, asesor o representante, o si manifiesta frustración que requiera escalamiento. IMPORTANTE: Llama a esta herramienta EN SILENCIO. NO generes ninguna respuesta",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "motivo": {
                            "type": "string",
                            "description": "Breve resumen de la conversación compelta."
                        }
                    },
                    "required": ["motivo"]
                }
            },
            {
                "type": "function",
                "name": "iniciar_autenticacion",
                "description": "Llama a esta función ESTRICTAMENTE SOLO UNA VEZ durante la conversación para solicitar la autenticación del usuario. NO la llames si el System Prompt indica que el usuario 'YA ESTÁ AUTENTICADO'. Si esta función ya fue ejecutada previamente en esta misma sesión, tienes prohibido volver a llamarla. IMPORTANTE: Llama a esta herramienta EN SILENCIO. NO generes ninguna respuesta",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "motivo": {
                            "type": "string",
                            "description": "El motivo por el cual se le pide al usuario que se autentique, usa solo una plabra para el motivo(ej. 'DATOS, BAJA, ALTA, COMPRA, BLOQUEAR')."
                        }
                    },
                    "required": ["motivo"]
                }
            },
            {
                "type": "function",
                "name": "finalizar_llamada",
                "description": "Llama a esta función cuando el usuario se despida, indique que su problema está resuelto, o pida explícitamente colgar o terminar la llamada. IMPORTANTE: Llama a esta herramienta EN SILENCIO. NO generes ninguna respuesta",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
            ],
            "tool_choice": "auto",
            "audio": {
                "input": {
                    "format": {
                        "type": "audio/pcmu"
                    },
                    "transcription": {
                        "model": "whisper-1"
                    },
                    "turn_detection": {
                        "type": "semantic_vad",
                        "eagerness": eagerness,
                        "create_response": True,
                        "interrupt_response": False
                    }
                },
                "output": {
                    "format": {
                        "type": "audio/pcmu",
                    },
                    "voice": open_ai_voice  # Nota: "marin" no es una voz estándar de OpenAI, revisa si querías usar alloy, echo, fable, onyx, nova o shimmer.
                }
            },
            "instructions": instructions
        }
        log.info(
            "→ SEND [session.update]  model=%s  input_fmt=%s  output_fmt=%s  vad=semantic_vad  eagerness=%s",
            model_name, audio_fmt, audio_fmt, eagerness,
        )
        await self._openai_send({"type": "session.update", "session": session_cfg})

        # Forzar saludo inicial sin esperar detección de voz del usuario
        log.info("→ SEND [response.create]  forzando saludo inicial")
        await self._openai_send({
            "type": "response.create",
            "response": {
                "instructions": welcome_instructions
            }
        })

        # Build streamer using the ingress bid for this endpoint
        bid = self._ingress_bid_by_endpoint.get(self._endpoint_id)
        if bid is None:
            log.warning(
                "No ingress bid found for endpoint=%s — defaulting to bid=1. "
                "Check that session.start carries mediaEndpoints with ingress targets.",
                self._endpoint_id,
            )
            bid = 1
        self._streamer = _IngressStreamer(ws=self.ws, bid=bid)
        log.info("_IngressStreamer created  bid=%d  endpoint=%s", bid, self._endpoint_id)

        self._tasks.append(asyncio.create_task(self._openai_event_loop()))
        #self._tasks.append(asyncio.create_task(self._buffer_flush_loop()))
        log.info("Background tasks started (event loop + buffer flush)")

    async def _openai_event_loop(self) -> None:
        log.info("OpenAI event loop iniciado")
        events_seen = 0
        try:
            async for raw_msg in self._openai_ws:
                events_seen += 1
                try:
                    event = json.loads(raw_msg)
                except json.JSONDecodeError as exc:
                    log.error("OpenAI bad JSON #%d: %s", events_seen, exc)
                    continue

                etype = event.get("type", "")
                # Log every event at INFO for debugging — tighten to DEBUG once stable
                log.info("OpenAI ← event #%d: %s", events_seen, etype)

                if etype == "session.created":
                    sess = event.get("session", {})
                    log.info(
                        "OpenAI session.created ✓  id=%s  model=%s  "
                        "input_fmt=%s  output_fmt=%s  vad=%s",
                        sess.get("id"), sess.get("model"),
                        sess.get("input_audio_format"), sess.get("output_audio_format"),
                        sess.get("turn_detection", {}).get("type"),
                    )

                elif etype == "session.updated":
                    sess = event.get("session", {})
                    log.info(
                        "OpenAI session.updated  input_fmt=%s  output_fmt=%s  vad=%s",
                        sess.get("input_audio_format"), sess.get("output_audio_format"),
                        sess.get("turn_detection", {}).get("type"),
                    )

                elif etype in ("response.output_audio.delta", "response.audio.delta"):
                    # Server streams base64 audio chunks (event name varies by model/version)
                    audio_bytes = base64.b64decode(event.get("delta", ""))
                    self._openai_audio_chunks += 1
                    if self._openai_audio_chunks == 1:
                        log.info("Primer audio de OpenAI — %d bytes  response_id=%s  event=%s",
                                 len(audio_bytes), event.get("response_id"), etype)
                    elif self._openai_audio_chunks % 50 == 0:
                        log.debug("OpenAI audio chunks=%d", self._openai_audio_chunks)
                    if self._streamer:
                        await self._streamer.queue_audio(audio_bytes)

                elif etype in ("response.output_audio.done", "response.audio.done"):
                    log.info("OpenAI audio done  chunks=%d  response_id=%s  event=%s",
                             self._openai_audio_chunks, event.get("response_id"), etype)
                    if self._streamer:
                        await self._streamer.mark_last()
                    self._openai_audio_chunks = 0

                elif etype == "input_audio_buffer.speech_started":
                    log.info("OpenAI: speech_started — barge-in  audio_start_ms=%s",
                             event.get("audio_start_ms"))
                    if self._streamer:
                        await self._streamer.barge_in()

                elif etype == "input_audio_buffer.speech_stopped":
                    log.info("OpenAI: speech_stopped  audio_end_ms=%s",
                             event.get("audio_end_ms"))

                elif etype == "input_audio_buffer.committed":
                    log.info("OpenAI: audio buffer committed  item_id=%s",
                             event.get("item_id"))

                elif etype == "response.created":
                    log.info("OpenAI: response.created  id=%s  status=%s",
                             event.get("response", {}).get("id"),
                             event.get("response", {}).get("status"))

                elif etype == "response.done":
                    resp = event.get("response", {})
                    log.info("OpenAI: response.done  id=%s  status=%s  usage=%s",
                             resp.get("id"), resp.get("status"), resp.get("usage"))

                elif etype == "response.output_item.added":
                    log.debug("OpenAI: output_item.added  item=%s",
                              event.get("item", {}).get("type"))

                elif etype == "response.content_part.added":
                    log.debug("OpenAI: content_part.added  type=%s",
                              event.get("part", {}).get("type"))

                elif etype == "response.audio_transcript.delta":
                    log.debug("OpenAI transcript delta: %s", event.get("delta", ""))

                elif etype == "response.output_audio_transcript.done":
                    texto = event.get("transcript", "")
                    log.info("UMA _ BOT_ Transcript: %s", texto)
                    resp = {
                        "version":     "1.0.0",
                        "type":        "bot.feature",
                        "sessionId":   self._session_id,
                        "sequenceNum": self._next_json_seq(),
                        "timestamp":   _iso_now(),
                        "payload": {
                            "ftype": "TRANSCRIPT",
                            "transcript": {
                                "turnId":     event.get("response_id", ""),
                                "speaker":    "BOT",
                                "isFinal":    True,
                                "text":       texto,
                                "confidence": 0.99,
                                "language":   "es-MX",
                                "startTsMs":  int(time.time() * 1000),
                                "context":    {},
                            },
                        },
                    }
                    await self._send_json(resp)

                elif etype == "conversation.item.input_audio_transcription.completed":
                    texto = event.get("transcript", "")
                    log.info("UMA _ CUSTOMER _ Transcript: %s", texto)
                    resp = {
                        "version":     "1.0.0",
                        "type":        "bot.feature",
                        "sessionId":   self._session_id,
                        "sequenceNum": self._next_json_seq(),
                        "timestamp":   _iso_now(),
                        "payload": {
                            "ftype": "TRANSCRIPT",
                            "transcript": {
                                "turnId":     event.get("item_id", ""),
                                "speaker":    "CUSTOMER",
                                "isFinal":    True,
                                "text":       texto,
                                "confidence": 0.99,
                                "language":   "es-MX",
                                "startTsMs":  int(time.time() * 1000),
                                "context":    {},
                            },
                        },
                    }
                    await self._send_json(resp)

                elif etype == "error":
                    err = event.get("error", event)
                    log.error(
                        "OpenAI API error — type=%s code=%s message=%s param=%s",
                        err.get("type"), err.get("code"),
                        err.get("message"), err.get("param"),
                    )

                elif etype == "response.function_call_arguments.done":
                    fn_name = event.get("name", "")
                    raw_args = event.get("arguments", "") or "{}"
                    call_id = event.get("call_id")
                    try:
                        fn_args = json.loads(raw_args)
                    except json.JSONDecodeError as exc:
                        log.error(
                            "OpenAI function_call_arguments.done — JSON inválido "
                            "name=%s err=%s raw=%r", fn_name, exc, raw_args,
                        )
                        fn_args = {}

                    log.info("OpenAI → tool call  name=%s  call_id=%s  args=%s",
                             fn_name, call_id, fn_args)

                    if fn_name == "transferir_a_agente":
                        motivo = fn_args.get("motivo", "")
                        log.info("Transfer a agente solicitada — motivo=%s", motivo)
                        resp = {
                            "version":     "1.0.0",
                            "type":        "bot.feature",
                            "sessionId":   self._session_id,
                            "sequenceNum": self._next_json_seq(),
                            "timestamp":   _iso_now(),
                            "payload": {
                                "ftype": "LIVE_AGENT_HANDOFF",
                                "liveAgentHandoff": {
                                    "context" : {
                                        "agentic_term" : fn_name.upper(),
                                        "agentic_summary" : motivo,
                                    },
                                    "queueId": "default-queue",
                                },
                            },
                        }
                        log.info("→ SEND [bot.feature LIVE_AGENT_HANDOFF]")
                        self._tasks.append(asyncio.create_task(self._send_delayed_action(resp, delay_seconds=2.5)))

                    if fn_name == "iniciar_autenticacion":
                        motivo = fn_args.get("motivo", "")
                        log.info("Autenticación solicitada — motivo=%s", motivo)
                        resp = {
                            "version":     "1.0.0",
                            "type":        "bot.feature",
                            "sessionId":   self._session_id,
                            "sequenceNum": self._next_json_seq(),
                            "timestamp":   _iso_now(),
                            "payload": {
                                "ftype": "LIVE_AGENT_HANDOFF",
                                "liveAgentHandoff": {
                                    "context" : {
                                        "agentic_term" : fn_name.upper(),
                                        "agentic_reason" : motivo,
                                    },
                                    "queueId": "default-queue",
                                },
                            },
                        }
                        log.info("→ SEND [bot.feature LIVE_AGENT_HANDOFF]")
                        self._tasks.append(asyncio.create_task(self._send_delayed_action(resp, delay_seconds=2.5)))

                    elif fn_name == "finalizar_llamada":
                        log.info("Finalizar llamada solicitada por el modelo")
                        resp = {
                            "version":     "1.0.0",
                            "type":        "bot.end",
                            "sessionId":   self._session_id,
                            "sequenceNum": self._next_json_seq(),
                            "timestamp":   _iso_now(),
                            "payload": {
                                "endpointId": self._endpoint_id,
                                "status": {
                                    "code":   200,
                                    "reason": "ENDPOINT_RELEASED",
                                },
                            },
                        }
                        log.info("→ SEND [bot.end ENDPOINT_RELEASED]")
                        self._tasks.append(asyncio.create_task(self._send_delayed_action(resp, delay_seconds=2.5)))

                    else:
                        log.warning("Tool call con nombre no manejado: %s  args=%s",
                                    fn_name, fn_args)

                else:
                    log.info("OpenAI event (no manejado): %s  full=%s",
                             etype, json.dumps(event))

        except asyncio.CancelledError:
            log.info("OpenAI event loop cancelado (%d eventos)", events_seen)
        except Exception as exc:
            log.error("OpenAI event loop crash: %s\n%s", exc, traceback.format_exc())
        log.info("OpenAI event loop salió — total eventos: %d", events_seen)

    # ── Audio buffering ──────────────────────────────────────────

    async def _flush_audio(self) -> None:
        if not self._audio_buf or self._openai_ws is None:
            return
        data = bytes(self._audio_buf)
        self._audio_buf.clear()
        self._last_flush = time.monotonic()
        self._audio_bytes_tx += len(data)
        b64 = base64.b64encode(data).decode()
        if self._audio_bytes_tx == len(data):
            log.info("Primer flush → OpenAI  bytes=%d  b64_len=%d", len(data), len(b64))
        await self._openai_send({"type": "input_audio_buffer.append", "audio": b64})

    async def _openai_send(self, msg: dict) -> None:
        if self._openai_ws is None:
            log.warning("_openai_send: WebSocket no abierto")
            return
        await self._openai_ws.send(json.dumps(msg))

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

    async def _send_delayed_action(self, payload: dict, delay_seconds: float = 2.5) -> None:
        """
        Espera en segundo plano para permitir que Whisper termine
        la transcripción del usuario y se envíe a Avaya primero.
        """
        log.info("Pausando %.1f seg para esperar la transcripción del usuario...", delay_seconds)
        await asyncio.sleep(delay_seconds)
        log.info("→ SEND (Delayed) [%s]", payload.get("type"))
        await self._send_json(payload)

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
