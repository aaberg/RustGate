#!/usr/bin/env python3
"""Exercise built images: CONTAINER_ENGINE=podman python3 scripts/container-smoke.py.

Requires Python 3.11+, OpenSSL, and a local Docker-compatible Linux engine.
RUSTGATE_IMAGE and RUSTGATE_MOCK_IMAGE default to rustgate:ci and rustgate-mock:ci.
The disposable HTTPS fixture uses python:3.13-slim-bookworm (pulled if absent).
All credentials are fake, mounted from temporary files, and redacted from diagnostics.
"""

import concurrent.futures
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


FIXTURE_IMAGE = "docker.io/library/python:3.13-slim-bookworm"
CONTROL_TOKEN = "fake-container-control-token-0123456789abcdef"
PROVIDER_TOKEN = "fake-container-provider-credential"
SYSTEM_PROMPT = "Container smoke operator prompt."
USER_MESSAGE = "Container smoke user message."

# Written only to the temporary fixture directory; never part of either image.
HTTPS_FIXTURE = r'''
import http.server
import json
import ssl

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ready")

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        expected = [
            {"role": "system", "content": "Container smoke operator prompt."},
            {"role": "user", "content": "Container smoke user message."},
        ]
        valid = (
            self.path == "/v1/chat/completions"
            and self.headers.get("Authorization") == "Bearer fake-container-provider-credential"
            and body.get("messages") == expected
        )
        response = json.dumps({
            "id": "https-fixture",
            "choices": [{"message": {"role": "assistant", "content": "Trusted HTTPS upstream."}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        }).encode()
        self.send_response(200 if valid else 400)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

server = http.server.ThreadingHTTPServer(("127.0.0.1", 9443), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/fixtures/upstream/server.pem", "/fixtures/upstream/server.key")
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
'''


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class Smoke:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.engine = os.environ.get("CONTAINER_ENGINE", "docker")
        self.gateway_image = os.environ.get("RUSTGATE_IMAGE", "rustgate:ci")
        self.mock_image = os.environ.get("RUSTGATE_MOCK_IMAGE", "rustgate-mock:ci")
        self.prefix = "rustgate-smoke-" + uuid.uuid4().hex[:12]
        self.volume = self.prefix + "-state"
        self.containers = []
        self.secrets = [CONTROL_TOKEN, PROVIDER_TOKEN]
        self.gateway = None
        self.mock = None
        self.base_url = None
        self.context = None
        # Do not let host proxy configuration route loopback smoke traffic elsewhere.
        self.http = None

    def redact(self, text):
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def command(self, *args, timeout=60, check=True):
        result = subprocess.run(
            [self.engine, *args], capture_output=True, text=True, timeout=timeout
        )
        if check and result.returncode:
            # No request credentials ever enter argv, including exec commands.
            raise RuntimeError(self.redact(
                f"Container engine {args[0]} failed: {result.stderr.strip()}"
            ))
        return result

    def inspect(self, name):
        return json.loads(self.command("inspect", name).stdout)[0]

    def write(self, name, content):
        path = self.directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(0o644)
        return path

    def certificates(self, name):
        directory = self.directory / name
        directory.mkdir(mode=0o755)
        commands = [
            ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
             "-subj", f"/CN=RustGate smoke {name} CA", "-keyout", "ca.key", "-out", "ca.pem",
             "-addext", "basicConstraints=critical,CA:TRUE",
             "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
            ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
             "-keyout", "server.key", "-out", "server.csr"],
            ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
             "-set_serial", "1", "-days", "2", "-extfile", "extensions", "-out", "server.pem"],
        ]
        (directory / "extensions").write_text(
            "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            "subjectKeyIdentifier=hash\n"
            "authorityKeyIdentifier=keyid,issuer\n"
        )
        for command in commands:
            result = subprocess.run(
                ["openssl", *command], cwd=directory, capture_output=True, text=True, timeout=30
            )
            require(result.returncode == 0, "OpenSSL fixture generation failed")
        # Private fixture keys are deliberately readable by UID 10001 in containers.
        # The host temporary directory remains 0700 and contains no real credentials.
        for path in directory.iterdir():
            path.chmod(0o644)

    def prepare(self):
        self.certificates("gateway")
        self.certificates("upstream")
        self.write("control-token", CONTROL_TOKEN + "\n")
        self.write("upstream-keys", json.dumps({"smoke": PROVIDER_TOKEN}) + "\n")
        self.write("https-fixture.py", HTTPS_FIXTURE)
        self.context = ssl.create_default_context(cafile=str(self.directory / "gateway/ca.pem"))
        self.http = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=self.context)
        )
        if self.command("image", "inspect", FIXTURE_IMAGE, check=False).returncode:
            self.command("pull", FIXTURE_IMAGE, timeout=180)
        self.command("volume", "create", self.volume)
        # Explicit initialization works for Docker and rootless Podman alike.
        self.command(
            "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--cap-add", "CHOWN", "--cap-add", "FOWNER",
            "--security-opt", "no-new-privileges", "--user", "0:0",
            "--volume", f"{self.volume}:/state", FIXTURE_IMAGE,
            "python3", "-c", "import os; os.chown('/state', 10001, 10001); os.chmod('/state', 0o700)"
        )

    def config(self, name, upstream):
        return self.write(name, f'''listen_addr = "0.0.0.0:8443"
upstream_base_url = "{upstream}"
tls_cert_path = "/fixtures/gateway/server.pem"
tls_key_path = "/fixtures/gateway/server.key"
database_path = "/var/lib/rustgate/rustgate.sqlite3"
proxy_url = "https://localhost:8443/v1"
connect_timeout_seconds = 3
first_byte_timeout_seconds = 10
idle_timeout_seconds = 10
overall_timeout_seconds = 20
''')

    @staticmethod
    def hardened():
        return ["--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges"]

    def run(self, name, args, image, *command):
        # Register before starting, so even a partially failed run is cleaned up.
        self.containers.append(name)
        self.command("run", "--detach", "--name", name, *self.hardened(), *args, image, *command)
        return name

    def start_gateway(self, suffix, upstream, trust_upstream=False):
        name = self.prefix + "-" + suffix
        config = self.config(suffix + ".toml", upstream)
        args = [
            "--network", "bridge", "--publish", "127.0.0.1::8443",
            "--volume", f"{self.directory}:/fixtures:ro,z",
            "--volume", f"{self.volume}:/var/lib/rustgate",
            "--env", f"RUSTGATE_CONFIG=/fixtures/{config.name}",
            "--env", "RUSTGATE_CONTROL_PLANE_TOKEN_FILE=/fixtures/control-token",
            "--env", "RUSTGATE_UPSTREAM_KEYS_FILE=/fixtures/upstream-keys",
            "--env", "RUSTGATE_HEALTHCHECK_CA_PATH=/fixtures/gateway/ca.pem",
            "--env", "RUSTGATE_TLS_CERT_PATH=/fixtures/gateway/server.pem",
            "--env", "RUSTGATE_TLS_KEY_PATH=/fixtures/gateway/server.key",
        ]
        if trust_upstream:
            args += ["--env", "RUSTGATE_UPSTREAM_CA_CERT_PATH=/fixtures/upstream/ca.pem"]
        self.gateway = self.run(name, args, self.gateway_image)
        self.gateway_ready()
        metadata = self.inspect(name)
        require(metadata["HostConfig"]["ReadonlyRootfs"], "Gateway root filesystem is not read-only")
        require(self.command("exec", name, "id", "-u").stdout.strip() == "10001",
                "Gateway is not running as UID 10001")
        return name

    def gateway_ready(self):
        ports = self.inspect(self.gateway)["NetworkSettings"]["Ports"]["8443/tcp"]
        require(len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1",
                "Gateway must publish only one loopback host port")
        self.base_url = "https://127.0.0.1:" + ports[0]["HostPort"]
        self.poll("trusted gateway HTTPS health", lambda: self.request("/healthz")[1] == {"status": "ok"})

    def start_mock(self):
        self.mock = self.run(
            self.prefix + "-mock", ["--network", "container:" + self.gateway], self.mock_image
        )
        self.poll("mock readiness", lambda: self.mock_get("/healthz") == {"status": "ok"})
        require(not self.inspect(self.mock)["HostConfig"].get("PortBindings"),
                "Mock port must never be published")
        require(self.command("exec", self.mock, "id", "-u").stdout.strip() == "10001",
                "Mock is not running as UID 10001")

    def mock_get(self, path, container=None):
        result = self.command(
            "exec", container or self.gateway, "curl", "--silent", "--show-error", "--fail",
            "--noproxy", "*", "--max-time", "2", "http://127.0.0.1:9090" + path,
            timeout=5
        )
        return json.loads(result.stdout)

    def poll(self, label, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (RuntimeError, OSError, ValueError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise RuntimeError("Timed out waiting for " + label)

    def request(self, path, token=None, body=None, expected=200):
        headers = {}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + path, data=data, headers=headers)
        try:
            response = self.http.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status = response.status
            payload = response.read()
        require(status == expected, f"Unexpected HTTP status for {path.split('?')[0]}: {status}, expected {expected}")
        return status, json.loads(payload)

    def issue(self, max_requests=4):
        _, issued = self.request("/v1/jobs", CONTROL_TOKEN, {
            "job_id": str(uuid.uuid4()), "tenant_id": "container-smoke",
            "upstream_key_id": "smoke", "system_prompt": SYSTEM_PROMPT,
            "ttl_seconds": 300, "max_requests": max_requests,
            "max_tokens_total": 100000,
        }, expected=201)
        token = issued["token"]
        self.secrets.append(token)
        return issued["job_id"], token

    @staticmethod
    def chat(stream=False):
        return {
            "model": "mock-mistral", "max_tokens": 16, "stream": stream,
            # Deliberate duplicates prove the persisted prompt is deduplicated and prepended.
            "messages": [
                {"role": "user", "content": USER_MESSAGE},
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "system", "content": SYSTEM_PROMPT},
            ],
        }

    def assert_captures(self, token, expected_count, container=None):
        captures = self.mock_get("/__mock/requests", container)
        require(len(captures) == expected_count, "Unexpected mock capture count")
        for capture in captures:
            require(capture["authorization"] == "Bearer " + PROVIDER_TOKEN,
                    "Mock did not receive exactly the fake provider credential")
            encoded = json.dumps(capture)
            require(token not in encoded and CONTROL_TOKEN not in encoded,
                    "Client/control credential reached the upstream")
            require(capture["path"] == "/v1/chat/completions", "Unexpected upstream path")
            require(capture["body"]["messages"] == [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_MESSAGE},
            ], "Operator prompt was not deduplicated and prepended")
            if capture["body"].get("stream"):
                require(capture["body"]["stream_options"]["include_usage"] is True,
                        "Streaming usage was not requested")

    def usage(self, job_id, requests, tokens):
        _, usage = self.request("/v1/usage", CONTROL_TOKEN)
        job = next(item for item in usage["jobs"] if item["job_id"] == job_id)
        key = next(item for item in usage["keys"] if item["upstream_key_id"] == "smoke")
        return (job["requests_used"] == requests and key["requests_used"] == requests
                and key["tokens_used"] == tokens and key["tokens_reserved"] == 0
                and key["tokens_unreported"] == 0)

    def stream(self, token):
        request = urllib.request.Request(
            self.base_url + "/v1/chat/completions?delay_ms=3000",
            data=json.dumps(self.chat(stream=True)).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}
        )
        events = []
        with self.http.open(request, timeout=15) as response:
            require(response.status == 200, "SSE request failed")
            require(response.headers.get_content_type() == "text/event-stream", "Not an SSE response")
            for line in response:
                if line.startswith(b"data:"):
                    events.append((time.monotonic(), line[5:].strip()))
        require(len(events) == 4 and events[-1][1] == b"[DONE]", "Incomplete SSE event sequence")
        first, second, usage = [json.loads(event[1]) for event in events[:3]]
        require(first["choices"][0]["delta"]["content"] == "Hello ", "Incorrect first SSE chunk")
        require(second["choices"][0]["delta"]["content"] == "from mock.", "Incorrect second SSE chunk")
        require(usage["usage"]["total_tokens"] == 8, "Missing SSE usage report")
        # A buffered response produces adjacent timestamps and fails this condition.
        require(events[1][0] - events[0][0] >= 2.0, "SSE was buffered instead of incrementally delivered")

    def remove(self, name):
        self.command("rm", "--force", name)
        self.containers.remove(name)

    def restart(self):
        # A restarted gateway has a new network namespace. Never retain its old mock.
        self.remove(self.mock)
        self.command("restart", "--time", "20", self.gateway, timeout=30)
        self.gateway_ready()
        self.start_mock()

    def http_scenario(self):
        self.start_gateway("http", "http://127.0.0.1:9090")
        self.start_mock()
        job_id, token = self.issue()
        _, completion = self.request("/v1/chat/completions", token, self.chat())
        require(completion["choices"][0]["message"]["content"] == "Hello from mock.",
                "Proxied JSON did not come from mock")
        self.stream(token)
        self.assert_captures(token, 2)
        self.poll("durable JSON and SSE accounting", lambda: self.usage(job_id, 2, 16))
        self.restart()
        require(self.usage(job_id, 2, 16), "Counters changed across gateway restart")
        self.request("/v1/chat/completions", token, self.chat())
        self.poll("restarted job token accounting", lambda: self.usage(job_id, 3, 24))
        self.assert_captures(token, 1)

        # Observe admission at the mock before SIGTERM; completion after shutdown
        # is proof of draining, not merely of idle process termination.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            held = pool.submit(
                self.request, "/v1/chat/completions?mode=delayed&delay_ms=5000", token, self.chat()
            )
            self.poll("held mock request capture", lambda: len(self.mock_get("/__mock/requests")) == 2)
            require(not held.done(), "Held request completed before shutdown was exercised")
            stop = pool.submit(self.command, "stop", "--time", "20", self.gateway, timeout=30)
            # The process's own signal log proves SIGTERM arrived while the request is held.
            self.poll("SIGTERM drain log", lambda: '"event":"shutdown"' in
                      self.gateway_logs(), timeout=4)
            require(not held.done(), "Request did not remain held when SIGTERM was handled")
            _, completion = held.result(timeout=15)
            require(completion["usage"]["total_tokens"] == 8, "Drained request lost its response")
            stop.result(timeout=25)
        state = self.inspect(self.gateway)["State"]
        require(not state["Running"] and state["ExitCode"] == 0, "Gateway did not drain and exit cleanly")
        self.assert_captures(token, 2, self.mock)
        self.restart()
        require(self.usage(job_id, 4, 32), "Drained request accounting was not durable")
        self.request("/v1/chat/completions", token, self.chat(), expected=429)
        require(self.mock_get("/__mock/requests") == [], "Exhausted persistent job reached upstream")
        self.remove(self.mock)
        self.remove(self.gateway)

    def gateway_logs(self):
        result = self.command("logs", self.gateway)
        return result.stdout + result.stderr

    def https_scenario(self, trusted):
        suffix = "https-trusted" if trusted else "https-untrusted"
        self.start_gateway(suffix, "https://localhost:9443", trust_upstream=trusted)
        fixture = self.run(
            self.prefix + "-fixture", [
                "--network", "container:" + self.gateway,
                "--volume", f"{self.directory}:/fixtures:ro,z",
                "--user", "10001:10001",
                "--env", "PYTHONDONTWRITEBYTECODE=1",
            ], FIXTURE_IMAGE, "python3", "/fixtures/https-fixture.py"
        )
        # Probe the fixture with its CA. /healthz on the gateway is only liveness.
        self.poll("HTTPS upstream fixture readiness", lambda: self.command(
            "exec", self.gateway, "curl", "--silent", "--show-error", "--fail",
            "--noproxy", "*", "--max-time", "2", "--cacert", "/fixtures/upstream/ca.pem",
            "https://localhost:9443/healthz", timeout=5
        ).stdout == "ready")
        _, token = self.issue()
        _, response = self.request(
            "/v1/chat/completions", token, self.chat(), expected=200 if trusted else 502
        )
        if trusted:
            require(response["id"] == "https-fixture", "Trusted HTTPS upstream was not reached")
        else:
            require("error" in response, "Untrusted upstream certificate was not rejected")
        self.remove(fixture)
        self.remove(self.gateway)

    def cleanup(self):
        # Preserve useful diagnostics on failure without exposing dynamically issued tokens.
        if sys.exc_info()[0] is not None:
            for name in self.containers:
                try:
                    result = self.command("logs", "--tail", "60", name, check=False)
                    print(self.redact(f"--- {name} ---\n{result.stdout}{result.stderr}"), file=sys.stderr)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        for name in reversed(self.containers):
            try:
                self.command("rm", "--force", name, check=False)
            except (OSError, subprocess.TimeoutExpired):
                pass
        try:
            self.command("volume", "rm", "--force", self.volume, check=False)
        except (OSError, subprocess.TimeoutExpired):
            pass


def main():
    with tempfile.TemporaryDirectory(prefix="rustgate-container-smoke-") as directory:
        fixtures = Path(directory) / "fixtures"
        fixtures.mkdir(mode=0o755)
        smoke = Smoke(fixtures)
        try:
            smoke.prepare()
            smoke.http_scenario()
            smoke.https_scenario(trusted=True)
            smoke.https_scenario(trusted=False)
        except Exception as error:
            print("Container smoke failed: " + smoke.redact(str(error)), file=sys.stderr)
            raise
        finally:
            smoke.cleanup()
    print("Container smoke passed: HTTPS, JSON, incremental SSE, restart persistence, drain, upstream TLS.")


if __name__ == "__main__":
    main()
