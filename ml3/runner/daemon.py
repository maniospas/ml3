import os
import subprocess
import time


DAEMON_CODE = r"""
import os, socket, struct, cloudpickle, traceback, sys, time
from io import TextIOBase
from multiprocessing import shared_memory
import numpy as _np

os.environ["ML3_IN_DAEMON"] = "1"

# ----------------------------------------------------------------
# Environment variables passed by parent:
#   ML3_RPC_SOCK     — Unix socket path OR TCP port file
#   ML3_STREAM_SOCK  — same, for streaming
#   ML3_IS_WINDOWS   — "1" if running on Windows
#   ML3_TIMEOUT      — max seconds; 0 = no timeout
# ----------------------------------------------------------------

IS_WINDOWS = os.environ.get("ML3_IS_WINDOWS", "0") == "1"
RPC_SOCK = os.environ["ML3_RPC_SOCK"]
STREAM_SOCK = os.environ["ML3_STREAM_SOCK"]
TIMEOUT = float(os.environ.get("ML3_TIMEOUT", "0"))

# Resolve sockets
if IS_WINDOWS:
    # Windows: RPC_SOCK and STREAM_SOCK contain TCP port numbers
    rpc_host = "127.0.0.1"
    stream_host = "127.0.0.1"
    with open(RPC_SOCK, "r") as f:
        rpc_port = int(f.read().strip())
    with open(STREAM_SOCK, "r") as f:
        stream_port = int(f.read().strip())

    rpc_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rpc_srv.bind((rpc_host, rpc_port))
    rpc_srv.listen(1)

    stream_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stream_srv.bind((stream_host, stream_port))
    stream_srv.listen(1)

else:
    # POSIX: UNIX sockets
    if os.path.exists(RPC_SOCK):
        os.remove(RPC_SOCK)
    if os.path.exists(STREAM_SOCK):
        os.remove(STREAM_SOCK)

    rpc_srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    rpc_srv.bind(RPC_SOCK)
    rpc_srv.listen(1)

    stream_srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stream_srv.bind(STREAM_SOCK)
    stream_srv.listen(1)


# ----------------------------------------------------------------
# Streaming writer (frame-based S1)
# ----------------------------------------------------------------
class StreamForwarder(TextIOBase):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def write(self, s):
        if not isinstance(s, str):
            s = str(s)
        b = s.encode("utf-8")
        header = struct.pack("!I", len(b))
        try:
            self.conn.sendall(header + b)
        except:
            # Client might have closed early
            pass
        return len(s)

    def flush(self):
        return


# ----------------------------------------------------------------
# Main loop: accept RPC, accept stream, execute fn, send result
# ----------------------------------------------------------------
while True:
    rpc_conn, _ = rpc_srv.accept()
    stream_conn, _ = stream_srv.accept()

    try:
        # Read RPC header
        hdr = rpc_conn.recv(4)
        if not hdr:
            rpc_conn.close()
            stream_conn.close()
            continue
        (size,) = struct.unpack("!I", hdr)

        # Read call data
        blob = b""
        while len(blob) < size:
            part = rpc_conn.recv(size - len(blob))
            if not part:
                break
            blob += part

        fn, args, kwargs = cloudpickle.loads(blob)

        # Timeout handled manually
        start = time.time()

        # Redirect both stdout and stderr
        sys.stdout = sf = StreamForwarder(stream_conn)
        sys.stderr = sf

        try:
            # Execute
            while True:
                # Check timeout
                if TIMEOUT and (time.time() - start) > TIMEOUT:
                    raise TimeoutError("Execution timed out")

                # Run function
                os.environ["ML3_IN_DAEMON"] = "1"
                result = fn(*args, **kwargs)
                break

            # Zero-copy ndarray
            if (
                result.__class__.__module__ == "numpy"
                and result.__class__.__name__ == "ndarray"
                and result.flags["C_CONTIGUOUS"]
            ):
                shm = shared_memory.SharedMemory(create=True, size=result.nbytes)
                arr = _np.ndarray(result.shape, result.dtype, buffer=shm.buf)
                arr[:] = result

                payload = cloudpickle.dumps(
                    ("SHM", shm.name, result.shape, str(result.dtype))
                )
            else:
                payload = cloudpickle.dumps(("OK", result))

        except Exception as e:
            tb = traceback.format_exc()
            payload = cloudpickle.dumps(("ERR", e, tb))

        # Send result packet
        rpc_conn.sendall(struct.pack("!I", len(payload)))
        rpc_conn.sendall(payload)

    finally:
        rpc_conn.close()
        stream_conn.close()
"""

_daemons = {}  # venv_name -> { "rpc": path, "stream": path, "pid": pid }

def _is_windows():
    return os.name == "nt"


def start_daemon(venv_name, timeout_sec):
    """
    Start a daemon for this venv if not already running.
    Creates RPC and STREAM sockets/ports.
    Returns (rpc_sock_path, stream_sock_path).
    """
    if venv_name in _daemons:
        return _daemons[venv_name]["rpc"], _daemons[venv_name]["stream"]

    # Paths
    base = f".ml3/{venv_name}"
    venv_path = base
    rpc_path = base + ".rpc.sock"
    stream_path = base + ".stream.sock"

    python = (
        os.path.join(venv_path, "bin", "python")
        if not _is_windows()
        else os.path.join(venv_path, "Scripts", "python.exe")
    )

    # Write daemon code into temp file inside venv
    daemon_file = os.path.join(venv_path, "_ml3_daemon.py")
    with open(daemon_file, "w") as f:
        f.write(DAEMON_CODE)

    env = os.environ.copy()
    env["ML3_IS_WINDOWS"] = "1" if _is_windows() else "0"
    env["ML3_TIMEOUT"] = str(timeout_sec)

    if _is_windows():
        # Windows uses TCP ports (store ports in these files)
        import random
        rpc_port = random.randint(30000, 60000)
        stream_port = random.randint(30000, 60000)

        # Write ports to files so daemon can read them
        with open(rpc_path, "w") as f:
            f.write(str(rpc_port))
        with open(stream_path, "w") as f:
            f.write(str(stream_port))

        env["ML3_RPC_SOCK"] = rpc_path
        env["ML3_STREAM_SOCK"] = stream_path
        env["ML3_TCP_PORT"] = str(rpc_port)

        p = subprocess.Popen(
            [python, daemon_file],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    else:
        # POSIX: UNIX sockets
        env["ML3_RPC_SOCK"] = rpc_path
        env["ML3_STREAM_SOCK"] = stream_path

        p = subprocess.Popen(
            [python, daemon_file],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    # Wait for daemon to create sockets/ports
    timeout = time.time() + 10
    while time.time() < timeout:
        if os.path.exists(rpc_path) and os.path.exists(stream_path):
            break
        time.sleep(0.01)

    _daemons[venv_name] = {
        "rpc": rpc_path,
        "stream": stream_path,
        "pid": p.pid,
    }

    return rpc_path, stream_path
