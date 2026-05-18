"""
WebRTC camera streaming server.

Captures the local camera, creates a WebRTC offer for each connecting viewer,
and drives the full signaling exchange over WebSocket — no separate publisher
process needed.

Usage:
    python stream_server.py [--host 0.0.0.0] [--port 8080] [--camera 0] [--record]

Endpoints:
    GET  /     serves webclient/dist/index.html (run `npm run build` first)
    WS   /ws   signaling for each viewer
"""


# Imports
import argparse
import asyncio
import fractions
import json
import logging
from datetime import datetime
from pathlib import Path
import platform
import cv2
import numpy as np
from aiohttp import WSMsgType, web
from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
from av import VideoFrame


# Config logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stream_server")


# Webclient directory
WEBCLIENT_DIST = Path(__file__).parent / "webclient" / "dist"


class Recorder:
    """
    Wraps cv2.VideoWriter to save raw camera frames to an .mp4 file.

    Created once at startup when --record is passed; each CameraTrack calls
    write() with the BGR frame before any overlays are burned in so the saved
    file is clean. Only the first active track writes to avoid duplicate frames
    when multiple viewers are connected simultaneously.
    """

    def __init__(self, width: int, height: int, fps: float = 30.0) -> None:
        # Build a timestamped filename so recordings never overwrite each other
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = Path(f"recording_{ts}.mp4")

        # mp4v codec writes a valid .mp4 on all platforms without extra deps
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        self._writer = cv2.VideoWriter(str(self.path), fourcc, fps, (width, height))
        self._active = True

        # Track how many CameraTrack instances are currently writing so only
        # the first one that calls acquire() actually writes frames.
        self._writers = 0

        log.info("Recording → %s  (%dx%d @ %.0f fps)", self.path, width, height, fps)

    def acquire(self) -> bool:
        """Returns True for the first caller — that track owns the recording."""
        self._writers += 1
        return self._writers == 1

    def release_track(self) -> None:
        """Called when a CameraTrack stops; lets the next viewer become the writer."""
        self._writers = max(0, self._writers - 1)

    def write(self, bgr: np.ndarray) -> None:
        """Write one BGR frame. No-op if stopped."""
        if self._active:
            self._writer.write(bgr)

    def stop(self) -> None:
        """Flush and close the file. Called on server shutdown."""
        if self._active:
            self._active = False
            self._writer.release()
            log.info("Recording saved → %s", self.path)


# Camera
class CameraTrack(VideoStreamTrack):
    """Reads frames from a local camera and yields them as VideoFrames for WebRTC."""

    def __init__(self, camera_id: int, recorder: Recorder | None = None) -> None:
        super().__init__()
        # Force AVFoundation backend on macOS — cv2 and av (PyAV) both ship libavdevice
        # and the duplicate symbols conflict when both are loaded, causing read() to fail
        # silently. CAP_AVFOUNDATION bypasses libav entirely and talks to the OS directly.
        backend = cv2.CAP_AVFOUNDATION if platform.system() == "Darwin" else cv2.CAP_ANY
        self.capture = cv2.VideoCapture(camera_id, backend)
        if not self.capture.isOpened():
            raise RuntimeError(f"Cannot open camera {camera_id}")

        # Cache frame dimensions so the placeholder frame matches the real resolution
        self._width = int(self.capture.get(cv2.CAP_PROP_FRAME_WIDTH)) or 640
        self._height = int(self.capture.get(cv2.CAP_PROP_FRAME_HEIGHT)) or 480

        # acquire() returns True only for the first track — that track writes to disk
        self._recorder = recorder
        self._is_primary_recorder = recorder.acquire() if recorder else False

    def stop(self) -> None:
        # Called by aiortc when the track ends — release the camera so it can be
        # reopened cleanly on reconnect (without this, the OS may keep it locked).
        self.capture.release()
        if self._recorder and self._is_primary_recorder:
            self._recorder.release_track()
        super().stop()

    async def recv(self) -> VideoFrame:
        # next_timestamp() paces frame delivery AND returns the pts/time_base in the
        # 90 kHz RTP clock domain that the decoder expects. We must use these values —
        # using our own counter with Fraction(1,30) gives wrong timestamps and the
        # decoder renders only the first frame (appears as a still image).
        pts, time_base = await self.next_timestamp()

        # capture.read() grabs the next frame from the camera.
        # Returns two values: a success flag and the raw image as a NumPy array (H x W x 3, BGR).
        ready, background = self.capture.read()

        if not ready:
            # Instead of crashing the sender, return a black placeholder frame so the
            # connection stays alive and the viewer sees a blank screen rather than an error.
            log.warning("Camera read failed — sending placeholder frame")
            background = np.zeros((self._height, self._width, 3), dtype=np.uint8)
            cv2.putText(background, "No camera signal", (10, self._height // 2),
                        cv2.FONT_HERSHEY_SIMPLEX, 1.0, (255, 255, 255), 2)

        # Write the clean frame to disk BEFORE burning the timestamp overlay so
        # the recording doesn't have the debug text baked in.
        if self._recorder and self._is_primary_recorder:
            self._recorder.write(background)

        # Burn the current timestamp into the top-left corner of the frame
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        cv2.putText(background, ts, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)

        # Converting here to avoid blue/red channel swap on the viewer side.
        rgb = cv2.cvtColor(background, cv2.COLOR_BGR2RGB)

        # Wrap the NumPy array in an av.VideoFrame so aiortc can encode and packetize it.
        frame = VideoFrame.from_ndarray(rgb, format="rgb24")

        # Stamp the frame with the 90 kHz pts returned by next_timestamp() so the
        # RTP packetizer and remote decoder see a continuous, correctly-paced timeline.
        frame.pts = pts
        frame.time_base = time_base
        return frame


# WebSocket handler
async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    """
    One WebSocket connection = one viewer = one RTCPeerConnection.

    Signaling sequence:
      1. Server creates offer (non-trickle: waits for ICE gathering to finish)
      2. Server sends offer to viewer
      3. Viewer sends back answer
      4. Viewer may also send trickle ICE candidates (handled below)
    """
    # Get camera id and peer address
    camera_id: int = request.app["camera_id"]
    recorder: Recorder | None = request.app["recorder"]
    peer_addr = request.remote

    # Connect to websocket
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    log.info("Viewer connected: %s", peer_addr)

    # Add camera to the WebRTC stream
    pc = RTCPeerConnection()
    pc.addTrack(CameraTrack(camera_id, recorder))

    # Fired whenever the overall PeerConnection health changes.
    @pc.on("connectionstatechange")
    async def _state():
        log.info("[%s] connection state: %s", peer_addr, pc.connectionState)

    # --- Build and send the offer ---
    # createOffer() generates the SDP (Session Description Protocol) blob that describes:
    #   - which codecs we support (e.g. H.264, VP8) and their parameters
    #   - the DTLS fingerprint used to authenticate the encrypted media channel
    #   - RTP payload type mappings
    offer = await pc.createOffer()

    # setLocalDescription() registers the offer on our side and starts ICE gathering
    await pc.setLocalDescription(offer)

    # We wait for ICE gathering to fully complete before sending the offer so the
    # viewer gets a complete SDP in one shot (avoids trickle ICE from our side).
    gathered = asyncio.get_event_loop().create_future()

    @pc.on("icegatheringstatechange")
    def _ice():
        # ICE gathering states: new → gathering → complete
        # "complete" means all local network paths have been discovered and added to the SDP.
        if pc.iceGatheringState == "complete" and not gathered.done():
            gathered.set_result(None)

    if pc.iceGatheringState != "complete":
        await asyncio.wait_for(gathered, timeout=15)

    # localDescription now contains the full SDP with all candidates embedded.
    await ws.send_json({"type": "offer", "sdp": pc.localDescription.sdp})
    log.info("[%s] offer sent", peer_addr)

    # Loop over every incoming WebSocket message until the viewer closes the connection.
    # Two message types are expected: "answer" (once) and "candidate" (zero or more).
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            mtype = data.get("type")

            if mtype == "answer":
                # The viewer's SDP answer completes the exchange; DTLS and RTP start after this.
                await pc.setRemoteDescription(
                    RTCSessionDescription(sdp=data["sdp"], type="answer")
                )
                log.info("[%s] answer applied — streaming", peer_addr)

            elif mtype == "candidate" and data.get("candidate"):
                # Browser may send trickle candidates even though our offer was non-trickle
                try:
                    await pc.addIceCandidate(RTCIceCandidate(
                        component=1,
                        foundation="0",
                        ip="0.0.0.0",
                        port=0,
                        priority=0,
                        protocol="udp",
                        type="host",
                        sdpMid=data.get("sdpMid"),
                        sdpMLineIndex=data.get("sdpMLineIndex"),
                    ))
                except Exception as e:
                    log.warning("[%s] addIceCandidate: %s", peer_addr, e)

    finally:
        # Always close the PC so camera/codec resources are released
        await pc.close()
        log.info("[%s] disconnected", peer_addr)

    return ws


async def index(_request: web.Request) -> web.FileResponse:
    # Serves the pre-built Vite SPA (webclient/dist/index.html) when a browser hits /.
    return web.FileResponse(WEBCLIENT_DIST / "index.html")


async def on_shutdown(app: web.Application) -> None:
    # Flush and close the recording file cleanly when the server receives Ctrl-C.
    recorder: Recorder | None = app.get("recorder")
    if recorder:
        recorder.stop()


def main() -> None:
    # Parse CLI args so the server can be pointed at different cameras or ports
    # without editing the source (e.g. python stream_server.py --camera 1 --port 9090)
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")  # 0.0.0.0 = listen on all network interfaces
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--camera", type=int, default=0)  # OS camera index (0 = first/default camera)
    p.add_argument("--record", action="store_true",  # off by default; pass flag to enable
                   help="Record camera output to a local .mp4 file")
    args = p.parse_args()

    # Open the camera once just to read its resolution for the Recorder.
    # CameraTrack will open its own capture per viewer connection.
    recorder: Recorder | None = None
    if args.record:
        backend = cv2.CAP_AVFOUNDATION if platform.system() == "Darwin" else cv2.CAP_ANY
        probe = cv2.VideoCapture(args.camera, backend)
        w = int(probe.get(cv2.CAP_PROP_FRAME_WIDTH)) or 640
        h = int(probe.get(cv2.CAP_PROP_FRAME_HEIGHT)) or 480
        probe.release()
        recorder = Recorder(width=w, height=h)

    app = web.Application()
    app["camera_id"] = args.camera
    app["recorder"] = recorder  # None when --record not passed

    app.router.add_get("/ws", ws_handler)
    app.on_shutdown.append(on_shutdown)  # ensures recorder.stop() on Ctrl-C

    # Optionally serve the pre-built viewer UI from the same server.
    if WEBCLIENT_DIST.exists():
        app.router.add_get("/", index)
        app.router.add_static("/assets", WEBCLIENT_DIST / "assets", show_index=False)
    else:
        log.warning("webclient/dist not found — run `cd webclient && npm run build` to serve the UI")

    log.info("Starting on http://%s:%d  recording=%s", args.host, args.port, args.record)
    # run_app() creates the asyncio event loop, binds the socket, and blocks until Ctrl-C.
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
