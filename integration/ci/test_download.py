"""Run with python3 integration/ci/test_download.py (requires bash and curl)."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
import shutil
import subprocess
from tempfile import TemporaryDirectory
from threading import Thread
import unittest


DOWNLOAD = Path(__file__).with_name("download.sh")
BASH = shutil.which("bash") or "bash"
PAYLOAD = b"a complete dependency archive\n" * 64


@contextmanager
def server(responses):
    class Handler(BaseHTTPRequestHandler):
        attempts = 0

        def do_GET(self):
            response = responses[min(Handler.attempts, len(responses) - 1)]
            Handler.attempts += 1
            if response == "unavailable":
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            if response == "partial":
                self.wfile.write(PAYLOAD[:100])
                self.wfile.flush()
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
            else:
                self.wfile.write(PAYLOAD)

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}/archive", Handler
    finally:
        httpd.shutdown()
        worker.join()
        httpd.server_close()


class DownloadTests(unittest.TestCase):
    def test_transient_http_failure(self):
        with TemporaryDirectory() as directory, server(["unavailable", "ok"]) as (url, handler):
            destination = Path(directory) / "archive"
            subprocess.run([BASH, str(DOWNLOAD), url, str(destination)], check=True, capture_output=True, timeout=30)
            self.assertEqual(destination.read_bytes(), PAYLOAD)
            self.assertEqual(handler.attempts, 2)

    def test_partial_transfer_is_replaced(self):
        with TemporaryDirectory() as directory, server(["partial", "ok"]) as (url, handler):
            destination = Path(directory) / "archive"
            subprocess.run([BASH, str(DOWNLOAD), url, str(destination)], check=True, capture_output=True, timeout=30)
            self.assertEqual(destination.read_bytes(), PAYLOAD)
            self.assertEqual(handler.attempts, 2)

    def test_permanent_failure_stops_after_retries(self):
        with TemporaryDirectory() as directory, server(["unavailable"]) as (url, handler):
            result = subprocess.run([BASH, str(DOWNLOAD), url, str(Path(directory) / "archive")], capture_output=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(handler.attempts, 4)


if __name__ == "__main__":
    unittest.main()
