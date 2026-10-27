# Public TLS test fixtures

The unencrypted server key and certificates are deliberately public test material,
not production credentials. No test changes the OS or browser trust store.

`tests/tls.py` uses Python's standard-library TLS server. Installed OpenSSL creates
a fresh seven-day CA and two-day positive leaf in each Rust-owned temporary
directory. These certificates cover localhost, 127.0.0.1 and ::1. The directory
and generated signing key are deleted after the fixture exits. Short lifetimes
also satisfy Secure Transport's certificate-lifetime policy.

OpenSSL is a development-test requirement, not a Piko runtime dependency. PATH,
`OPENSSL`, and Windows Git's `usr/bin/openssl.exe` are supported. No Python package,
external service or model credentials are required.

The static date-test CA is valid from 2020 until 2040. It signs the deliberately
expired (2021) and not-yet-valid (2099) leaves, which share the public fixture
server key. Its signing key was discarded. Clients trust it only for these
negative tests. Never use these fixtures for a deployment.
