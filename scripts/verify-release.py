"""Read-only verification of public npm/GitHub release payloads (stdlib only).

Run from any directory: python3 scripts/verify-release.py --version 0.7.0 --out REPORT.json
Downloads live only in a temporary directory and are removed on success or failure.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

REPO = Path(__file__).resolve().parent.parent
PLATFORMS = ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64", "win32-arm64", "win32-x64")
HOSTS = frozenset(("registry.npmjs.org", "api.github.com", "github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"))
MAX_ASSET = 40 * 1024 * 1024
SLSA_V1 = "https://slsa.dev/provenance/v1"
REPOSITORY = "https://github.com/ErzenXz/piko"


def safe_url(url, hosts=HOSTS):
    if not isinstance(url, str):
        raise ValueError("Download URL must be text")
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in hosts or parsed.port not in (None, 443)
            or parsed.username or parsed.password or parsed.fragment):
        raise ValueError("Unexpected download origin: " + parsed.netloc)
    return url


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        safe_url(new_url)
        # urllib never receives an authentication header in this public verifier.
        return super().redirect_request(request, fp, code, message, headers, new_url)


class Reader:
    def __init__(self, timeout):
        self.deadline = time.monotonic() + timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), SafeRedirect())

    def download(self, url, limit, path=None):
        safe_url(url)
        attempts = 0
        while True:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Public release visibility deadline reached; wait for CDN propagation rather than republishing")
            # Cache-bust the initial public URL only; signed redirects remain intact.
            parts = urllib.parse.urlsplit(url)
            query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
            query.append(("piko_verify", str(time.time_ns())))
            request_url = urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))
            request = urllib.request.Request(request_url, headers={
                "User-Agent": "Piko-release-verifier/1", "Cache-Control": "no-cache", "Pragma": "no-cache",
                "Accept": "application/vnd.github+json" if parts.hostname == "api.github.com" else "*/*",
            })
            try:
                with self.opener.open(request, timeout=min(30, remaining)) as response:
                    safe_url(response.url)
                    length = response.headers.get("Content-Length")
                    if length is not None and int(length) > limit:
                        raise ValueError("Download exceeds size limit")
                    total = 0
                    chunks = []
                    output = path.open("wb") if path else None
                    try:
                        while True:
                            if time.monotonic() >= self.deadline:
                                raise TimeoutError("Public release verification deadline reached")
                            chunk = response.read(min(64 * 1024, limit + 1 - total))
                            if not chunk:
                                break
                            total += len(chunk)
                            if total > limit:
                                raise ValueError("Download exceeds size limit")
                            if output:
                                output.write(chunk)
                            else:
                                chunks.append(chunk)
                    finally:
                        if output:
                            output.close()
                    if length is not None and total != int(length):
                        raise OSError("Incomplete public payload download")
                    return total if path else b"".join(chunks)
            except urllib.error.HTTPError as error:
                if error.code not in (404, 408, 429) and not 500 <= error.code <= 599:
                    raise
                reason = "HTTP " + str(error.code)
                error.close()
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
                reason = type(error).__name__
            attempts += 1
            if attempts == 1:
                print(f"Waiting for public CDN visibility ({reason}): {parts.path}", file=sys.stderr, flush=True)
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Public payload unavailable before verification deadline: " + parts.path)
            time.sleep(min(15, 2 + attempts, remaining))

    def json(self, url):
        return json.loads(self.download(url, 4 * 1024 * 1024))


def digest(path, algorithm="sha256"):
    result = hashlib.new(algorithm)
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            result.update(chunk)
    return result.digest()


def verify_script(script, arguments, cwd):
    result = subprocess.run([sys.executable, "-I", str(REPO / "scripts" / script), *map(str, arguments)],
                            cwd=cwd, capture_output=True, text=True, timeout=60, check=False)
    if result.returncode:
        raise ValueError(script + " rejected public payload: " + (result.stderr or result.stdout).strip()[-4000:])


def check(condition, message):
    if not condition:
        raise ValueError(message)


def approval_provenance(manifest, name, version, tarball_sha512, reader, source_commit=None):
    """Validate public registry metadata and DSSE claims, not Sigstore signatures.

    HTTPS registry transport is the trust boundary here. Signature, certificate,
    transparency-log and identity verification require a separate Sigstore verifier.
    """
    user = manifest.get("_npmUser")
    check(isinstance(user, dict), "Missing npm publishing identity: " + name)
    approver = user.get("approver")
    check(isinstance(approver, dict) and isinstance(approver.get("name"), str)
          and bool(approver["name"].strip()), "Missing npm staged approver: " + name)
    publisher = user.get("trustedPublisher")
    check(isinstance(publisher, dict) and publisher.get("id") == "github"
          and isinstance(publisher.get("oidcConfigId"), str) and bool(publisher["oidcConfigId"].strip()),
          "Missing GitHub trusted publisher configuration: " + name)
    distribution = manifest.get("dist")
    check(isinstance(distribution, dict), "Invalid registry distribution metadata: " + name)
    attestations = distribution.get("attestations")
    check(isinstance(attestations, dict) and isinstance(attestations.get("provenance"), dict)
          and attestations["provenance"].get("predicateType") == SLSA_V1,
          "Missing SLSA v1 provenance metadata: " + name)
    url = safe_url(attestations.get("url", ""), frozenset(("registry.npmjs.org",)))
    parsed = urllib.parse.urlsplit(url)
    check(not parsed.query and urllib.parse.unquote(parsed.path) == "/-/npm/v1/attestations/" + name + "@" + version,
          "Attestation URL does not bind package/version: " + name)
    document = reader.json(url)
    records = document.get("attestations") if isinstance(document, dict) else None
    check(isinstance(records, list) and 0 < len(records) <= 20, "Invalid attestation bundle collection: " + name)
    records = [record for record in records if isinstance(record, dict) and record.get("predicateType") == SLSA_V1]
    check(len(records) == 1, "Expected one SLSA v1 provenance bundle: " + name)
    bundle = records[0].get("bundle")
    envelope = bundle.get("dsseEnvelope") if isinstance(bundle, dict) else None
    check(isinstance(envelope, dict) and envelope.get("payloadType") == "application/vnd.in-toto+json",
          "Invalid provenance DSSE envelope: " + name)
    payload = envelope.get("payload")
    check(isinstance(payload, str) and len(payload) <= 3 * 1024 * 1024, "Invalid provenance payload: " + name)
    statement = json.loads(base64.b64decode(payload, validate=True))
    check(isinstance(statement, dict) and statement.get("_type") == "https://in-toto.io/Statement/v1"
          and statement.get("predicateType") == SLSA_V1, "Invalid SLSA v1 statement: " + name)
    subjects = statement.get("subject")
    check(isinstance(subjects, list) and len(subjects) <= 20, "Invalid provenance subjects: " + name)
    subjects = [subject for subject in subjects if isinstance(subject, dict)
                and isinstance(subject.get("name"), str)
                and urllib.parse.unquote(subject["name"]) == "pkg:npm/" + name + "@" + version]
    check(len(subjects) == 1 and isinstance(subjects[0].get("digest"), dict)
          and subjects[0]["digest"].get("sha512") == tarball_sha512,
          "Provenance subject SHA-512/package identity mismatch: " + name)
    predicate = statement.get("predicate")
    definition = predicate.get("buildDefinition") if isinstance(predicate, dict) else None
    check(isinstance(definition, dict), "Missing provenance build definition: " + name)
    external = definition.get("externalParameters")
    workflow = external.get("workflow") if isinstance(external, dict) else None
    check(isinstance(workflow, dict) and workflow.get("repository") == REPOSITORY
          and workflow.get("path") == ".github/workflows/release.yml" and workflow.get("ref") == "refs/tags/v" + version,
          "Provenance release workflow/repository/ref mismatch: " + name)
    dependencies = definition.get("resolvedDependencies")
    check(isinstance(dependencies, list) and len(dependencies) <= 100, "Invalid provenance source dependencies: " + name)
    sources = [dependency for dependency in dependencies if isinstance(dependency, dict)
               and dependency.get("uri") == "git+" + REPOSITORY + "@refs/tags/v" + version]
    check(len(sources) == 1 and isinstance(sources[0].get("digest"), dict), "Missing bound release source dependency: " + name)
    commit = sources[0]["digest"].get("gitCommit")
    check(isinstance(commit, str) and re.fullmatch(r"[0-9a-f]{40}", commit) is not None,
          "Invalid provenance source commit: " + name)
    check(source_commit is None or commit == source_commit, "Provenance source commit mismatch: " + name)
    return {"approver": approver["name"], "trustedPublisher": {"id": "github", "oidcConfigId": publisher["oidcConfigId"]},
            "provenance": {"predicateType": SLSA_V1, "url": url, "sourceCommit": commit,
                           "workflow": workflow, "subjectSha512": tarball_sha512,
                           "validation": "Registry metadata and decoded DSSE claims; no cryptographic Sigstore verification"}}


def verify(version, timeout, require_approval=False, source_commit=None):
    reader = Reader(timeout)
    with tempfile.TemporaryDirectory(prefix="piko-public-verification-") as directory:
        work = Path(directory)
        # Existing validators deliberately compare against the trusted checkout's
        # launcher contract. Isolate a version-adjusted expected manifest so this
        # verifier can also validate an older release without modifying the repo.
        cli = json.loads((REPO / "npm/cli/package.json").read_text())
        cli["version"] = version
        cli["optionalDependencies"] = {"@erzenx/piko-" + platform: version for platform in PLATFORMS}
        (work / "npm/cli").mkdir(parents=True)
        (work / "npm/cli/package.json").write_text(json.dumps(cli))
        npm = {}
        paths = {}
        release_source = source_commit
        for platform in (*PLATFORMS, "cli"):
            name = "@erzenx/piko" + ("" if platform == "cli" else "-" + platform)
            manifest = reader.json("https://registry.npmjs.org/" + urllib.parse.quote(name, safe="") + "/" + urllib.parse.quote(version, safe=""))
            check(manifest.get("name") == name and manifest.get("version") == version, "Registry identity mismatch")
            dist = manifest.get("dist", {})
            check(isinstance(dist, dict), "Invalid registry distribution metadata")
            url = safe_url(dist.get("tarball", ""), frozenset(("registry.npmjs.org",)))
            integrity = dist.get("integrity", "")
            check(isinstance(integrity, str), "Invalid npm integrity metadata")
            file_name = f"erzenx-piko-{version}.tgz" if platform == "cli" else f"native-{platform}.tgz"
            path = work / file_name
            reader.download(url, MAX_ASSET, path)
            actual = "sha512-" + base64.b64encode(digest(path, "sha512")).decode("ascii")
            check(actual in integrity.split(), "npm SHA-512 mismatch for " + name)
            verify_script("verify-package.py", (path, platform), work)
            entry = {"name": name, "version": version, "tarballBytes": path.stat().st_size,
                     "integrity": actual, "tarballSha256": digest(path).hex()}
            if platform != "cli":
                binary = "package/bin/piko" + (".exe" if platform.startswith("win32") else "")
                with tarfile.open(path, "r:gz") as package:
                    member = package.getmember(binary)
                    entry["binaryBytes"] = member.size
                    entry["binarySha256"] = hashlib.file_digest(package.extractfile(member), "sha256").hexdigest()
            attestations = dist.get("attestations")
            if isinstance(attestations, dict):
                entry["attestations"] = attestations
            if require_approval:
                entry["approval"] = approval_provenance(manifest, name, version, digest(path, "sha512").hex(), reader, release_source)
                release_source = entry["approval"]["provenance"]["sourceCommit"]
            npm[platform] = entry
            paths[file_name] = path
            print("Verified public npm " + name + "@" + version, file=sys.stderr, flush=True)

        tag = "v" + version
        release = reader.json("https://api.github.com/repos/ErzenXz/piko/releases/tags/" + urllib.parse.quote(tag, safe=""))
        check(release.get("tag_name") == tag and not release.get("draft"), "GitHub release identity/draft mismatch")
        archive_names = {platform: f"piko-{version}-{platform}." + ("zip" if platform.startswith("win32") else "tar.gz") for platform in PLATFORMS}
        expected = set(paths) | set(archive_names.values()) | {"SHA256SUMS"}
        assets = {}
        for asset in release.get("assets", []):
            name, size = asset.get("name"), asset.get("size")
            check(name in expected and name not in assets, "Unexpected/duplicate release asset")
            check(type(size) is int and 0 < size <= MAX_ASSET, "Invalid release asset size")
            assets[name] = asset
        check(set(assets) == expected and len(assets) == 14, "Expected exactly 14 public release assets")
        public_paths = {}
        for name, asset in assets.items():
            url = safe_url(asset.get("browser_download_url", ""), frozenset(("github.com",)))
            parsed = urllib.parse.urlsplit(url)
            expected_path = "/ErzenXz/piko/releases/download/" + tag + "/" + name
            check(urllib.parse.unquote(parsed.path) == expected_path, "Unexpected GitHub asset path")
            path = work / ("github-" + name)
            reader.download(url, 64 * 1024 if name == "SHA256SUMS" else MAX_ASSET, path)
            check(path.stat().st_size == asset["size"], "GitHub asset size mismatch: " + name)
            github_digest = asset.get("digest")
            if github_digest:
                check(github_digest == "sha256:" + digest(path).hex(), "GitHub asset digest mismatch: " + name)
            public_paths[name] = path
        sums = {}
        for line in public_paths["SHA256SUMS"].read_text(encoding="ascii").splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
            check(match is not None, "Invalid SHA256SUMS line")
            checksum, name = match.groups()
            check(name in expected - {"SHA256SUMS"} and name not in sums, "Unexpected/duplicate checksummed asset")
            sums[name] = checksum
        check(set(sums) == expected - {"SHA256SUMS"} and len(sums) == 13, "Expected exactly 13 payload checksums")
        for name, checksum in sums.items():
            check(digest(public_paths[name]).hex() == checksum, "GitHub SHA256SUMS mismatch: " + name)
        for name, npm_path in paths.items():
            check(digest(npm_path) == digest(public_paths[name]), "GitHub/npm tarball mismatch: " + name)
        for platform, name in archive_names.items():
            verify_script("verify-archive.py", (public_paths[name], paths[f"native-{platform}.tgz"], platform), work)
        return {"version": version, "verifiedAt": datetime.now(timezone.utc).isoformat(), "npm": npm,
                "approvalPolicy": {"required": require_approval, "sourceCommit": release_source,
                    "validation": "Public registry metadata and DSSE claims, not cryptographic Sigstore verification"},
                "github": {"url": release.get("html_url"), "tag": tag, "publishedAt": release.get("published_at"),
                           "assets": {name: {"bytes": assets[name]["size"], "sha256": digest(path).hex()} for name, path in sorted(public_paths.items())}},
                "checks": {"npmSha512": 7, "packageAllowlists": 7, "releaseAssets": 14,
                           "checksummedPayloads": 13, "registryGithubTarballParity": 7, "standaloneNativeParity": 6,
                           "stagedApprovals": 7 if require_approval else 0,
                           "githubTrustedPublishers": 7 if require_approval else 0,
                           "slsaV1ClaimBindings": 7 if require_approval else 0}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=1200, help="Global visibility deadline, 1–1200 seconds (default: 1200)")
    parser.add_argument("--require-approval", action="store_true", help="Require staged approvers and bound GitHub/SLSA v1 metadata for all seven packages")
    parser.add_argument("--source-commit", help="Expected 40-character release Git commit (requires --require-approval)")
    arguments = parser.parse_args()
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", arguments.version):
        parser.error("Invalid semantic release version")
    if not 1 <= arguments.timeout <= 1200:
        parser.error("--timeout must be between 1 and 1200 seconds")
    if arguments.source_commit and (not arguments.require_approval or not re.fullmatch(r"[0-9a-f]{40}", arguments.source_commit)):
        parser.error("--source-commit requires --require-approval and a lowercase 40-character Git commit")
    report = verify(arguments.version, arguments.timeout, arguments.require_approval, arguments.source_commit)
    arguments.out.parent.mkdir(parents=True, exist_ok=True)
    # Never leave a success-looking partial report after an interrupted write.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", prefix=".piko-verification-", dir=arguments.out.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            json.dump(report, output, indent=2)
            output.write("\n")
            output.flush()
            output.close()
            temporary.replace(arguments.out)
        finally:
            temporary.unlink(missing_ok=True)
    print(json.dumps({"version": arguments.version, "verified": True, "checks": report["checks"], "report": str(arguments.out.resolve())}, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, tarfile.TarError, subprocess.SubprocessError) as error:
        print("Release verification failed: " + str(error), file=sys.stderr)
        sys.exit(1)
