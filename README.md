# WebRTC Camera Stream Server

Python server that captures the local camera and streams it over WebRTC to any browser on the same network. Designed to run on a **Unitree G1 robot** and stream to a **Meta Quest** browser.

## Requirements

- Python 3.11+
- A connected camera

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

If any package fails to build on ARM64 (G1), install system deps first:
```bash
sudo apt install python3-dev libavformat-dev libavcodec-dev libavdevice-dev
```

## Run

```bash
python stream_server.py --camera 0 --port 8080
```

Enable recording (saves `recording_YYYYMMDD_HHMMSS.mp4` next to the script):
```bash
python stream_server.py --camera 0 --record
```

Binds to `0.0.0.0:8080`. Find the robot's LAN IP:
```bash
hostname -I
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Interface to bind |
| `--port` | `8080` | Port to listen on |
| `--camera` | `0` | OS camera index |
| `--record` | off | Save camera output to a local `.mp4` |

## Viewer

Use the companion repo **WebRTC-viewer** to connect from a browser on the same network.

**Option A — serve the viewer from the robot** (recommended for Quest):
```bash
# build on your machine
cd ../WebRTC-viewer && npm run build

# copy to robot
scp -r dist/ user@<robot-ip>:~/WebRTC-basic/webclient/dist/
```
Then open `http://<robot-ip>:8080` in the Quest browser.

**Option B — run the viewer dev server separately** on any machine on the same network:
```bash
cd ../WebRTC-viewer && npm run dev -- --host
```
Open `http://<that-machine-ip>:5173` in the Quest browser, enter `ws://<robot-ip>:8080/ws`.

## Testing locally before deploying

```bash
# terminal 1
python stream_server.py --camera 0

# terminal 2
cd ../WebRTC-viewer && npm run dev
```

Open `http://localhost:5173`, connect to `ws://localhost:8080/ws`.

## Troubleshooting

| Issue | Fix |
|---|---|
| Port 8080 blocked | `sudo ufw allow 8080` |
| Wrong camera | Try `--camera 1` or `--camera 2` |
| ARM build fails | Install system deps above |
| Quest browser blocks `ws://` | Add TLS — self-signed cert + `wss://` |
