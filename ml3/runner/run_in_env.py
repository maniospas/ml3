import cloudpickle
import struct
import socket
from multiprocessing import shared_memory
import threading
from ml3.runner.daemon import start_daemon, _is_windows


def _connect_rpc(rpc_path):
    """
    Connect to the RPC socket (Unix or TCP).
    Returns socket.
    """
    if _is_windows():
        # rpc_path contains a file with a port
        with open(rpc_path, "r") as f:
            port = int(f.read().strip())
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
        return s
    else:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(rpc_path)
        return s


def _connect_stream(stream_path):
    """
    Connect to the streaming socket.
    Returns socket.
    """
    if _is_windows():
        # stream_path contains a TCP port
        with open(stream_path, "r") as f:
            port = int(f.read().strip())
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
        return s
    else:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(stream_path)
        return s


def _stream_reader_thread(sock):
    """
    Reads framed S1 log chunks from daemon and prints them immediately.
    """
    try:
        while True:
            header = sock.recv(4)
            if not header:
                break
            (size,) = struct.unpack("!I", header)
            chunk = b""
            while len(chunk) < size:
                part = sock.recv(size - len(chunk))
                if not part:
                    break
                chunk += part

            try:
                text = chunk.decode("utf-8", errors="replace")
            except:
                text = "<BINARY_LOG>\n"

            print(text, end="", flush=True)
    except:
        pass
    finally:
        try:
            sock.close()
        except:
            pass


def run_in_venv(venv_name, fn, *args, timeout=0, **kwargs):
    rpc_path, stream_path = start_daemon(venv_name, timeout)

    # Connect RPC + STREAM
    rpc_sock = _connect_rpc(rpc_path)
    stream_sock = _connect_stream(stream_path)

    # Start streaming thread
    t = threading.Thread(target=_stream_reader_thread, args=(stream_sock,), daemon=True)
    t.start()

    # Serialize call
    blob = cloudpickle.dumps((fn, args, kwargs))
    rpc_sock.sendall(struct.pack("!I", len(blob)) + blob)

    # Receive RPC response
    header = rpc_sock.recv(4)
    if not header:
        rpc_sock.close()
        raise RuntimeError("Daemon closed RPC connection unexpectedly")

    (size,) = struct.unpack("!I", header)
    payload = b""
    while len(payload) < size:
        part = rpc_sock.recv(size - len(payload))
        if not part:
            break
        payload += part

    rpc_sock.close()

    # Decode result
    status, *rest = cloudpickle.loads(payload)

    if status == "OK":
        result, = rest
        return result

    if status == "SHM":
        shm_name, shape, dtype = rest
        import numpy as _np
        shm = shared_memory.SharedMemory(name=shm_name)
        arr = _np.ndarray(shape, dtype, buffer=shm.buf)
        return arr

    if status == "ERR":
        exc, tb = rest
        raise RuntimeError(f"Exception inside venv:\n{tb}") from exc

    raise RuntimeError("Unknown response from daemon")