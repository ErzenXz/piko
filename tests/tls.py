"""Loopback-only HTTPS fixture used by tests/tls.rs.

The private key is intentionally public test material. No OS trust store is
changed, no external endpoint is contacted, and no model or host tool is run.
An installed OpenSSL executable generates short-lived positive test leaves;
this is a development-test requirement, not a Piko runtime dependency.
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import time


def openssl_program():
    candidates = [os.environ.get("OPENSSL"), shutil.which("openssl")]
    if os.name == "nt":
        git = shutil.which("git")
        if git:
            candidates.append(str(Path(git).resolve().parent.parent / "usr" / "bin" / "openssl.exe"))
        for name in ["ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"]:
            if base := os.environ.get(name):
                candidates.append(str(Path(base) / "Git" / "usr" / "bin" / "openssl.exe"))
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise RuntimeError("TLS tests require installed OpenSSL (Windows Git usr/bin is supported)")


def positive_certificate(directory, public_server_key):
    """Keep the positive leaf below Secure Transport's 398-day maximum.

    Rust owns this scratch directory, so killing the server cannot leak private
    CA material. The CA is trusted only by its matching per-test client.
    """
    program = openssl_program()
    config = directory / "openssl.cnf"
    config.write_text("""[req]
distinguished_name=dn
prompt=no
[dn]
CN=Piko ephemeral test CA
[root]
basicConstraints=critical,CA:TRUE,pathlen:0
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
""")
    extensions = directory / "leaf.ext"
    extensions.write_text("""basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
""")

    def run(*args):
        subprocess.run([program, *map(str, args)], check=True, timeout=10,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    ca, certificate = directory / "ca.pem", directory / "server.pem"
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256", "-days", "7",
        "-config", config, "-extensions", "root", "-keyout", directory / "ca.key", "-out", ca)
    run("req", "-new", "-key", public_server_key, "-subj", "/CN=localhost",
        "-config", config, "-out", directory / "server.csr")
    run("x509", "-req", "-in", directory / "server.csr", "-CA", ca, "-CAkey", directory / "ca.key",
        "-set_serial", "2", "-days", "2", "-sha256", "-extfile", extensions, "-out", certificate)
    return ca, certificate


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def send(self, payload, mime="application/json"):
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()

    def do_GET(self):
        if self.path == "/slow":
            time.sleep(2)
        if self.path == "/stall":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b": ready\n\n")
            self.wfile.flush()
            time.sleep(5)
            return
        self.send(json.dumps({"ok": True, "tls": self.connection.version()}).encode())

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 65536:
            self.send_error(400)
            return
        request = json.loads(self.rfile.read(length))
        if any(item.get("type") == "function_call_output" for item in request.get("input", [])):
            output = [{"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "TLS ✓ — tool result received"}]}]
        else:
            output = [{"type": "function_call", "call_id": "tls-tool", "name": "read_file",
                       "arguments": json.dumps({"path": "tls-fixture.txt"})}]
        event = {"type": "response.completed", "response": {"output": output}}
        payload = ("data: " + json.dumps(event, ensure_ascii=False) + "\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        # Split UTF-8 and SSE event boundaries, rather than relying on one read.
        for start in range(0, len(payload), 7):
            self.wfile.write(payload[start:start + 7])
            self.wfile.flush()


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, _request, _client_address):
        # Timeout/cancellation tests deliberately close sockets mid-response.
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tls-version", choices=["1.2", "1.3", "both"], default="both")
    parser.add_argument("--certificate", choices=["server", "expired", "future"], default="server")
    parser.add_argument("--temporary-directory", type=Path)
    args = parser.parse_args()
    fixtures = Path(__file__).parent / "fixtures" / "tls"
    temporary = None
    if args.temporary_directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="piko-tls-fixture-")
        directory = Path(temporary.name)
    else:
        directory = args.temporary_directory
    if args.certificate == "server":
        ca, certificate = positive_certificate(directory, fixtures / "server.key")
    else:
        ca, certificate = fixtures / "date-test-ca.pem", fixtures / (args.certificate + ".pem")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if args.tls_version == "1.2":
        context.maximum_version = ssl.TLSVersion.TLSv1_2
    elif args.tls_version == "1.3":
        context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certificate, fixtures / "server.key")
    with Server(("127.0.0.1", 0), Handler) as server:
        server.socket = context.wrap_socket(server.socket, server_side=True)
        print(json.dumps({"port": server.server_port, "ca": str(ca)}), flush=True)
        server.serve_forever(poll_interval=.02)
    if temporary is not None:
        temporary.cleanup()


if __name__ == "__main__":
    main()
