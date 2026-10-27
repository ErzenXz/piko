"""Offline fixtures for strict public approval/provenance claim validation."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("verify_release", Path(__file__).with_name("verify-release.py"))
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)

NAME, VERSION, COMMIT, SHA512 = "@erzenx/piko", "0.7.1", "a" * 40, "b" * 128
URL = "https://registry.npmjs.org/-/npm/v1/attestations/@erzenx%2fpiko@0.7.1"


def fixtures():
    manifest = {"_npmUser": {"approver": {"name": "erzenx"}, "trustedPublisher": {
        "id": "github", "oidcConfigId": "package-specific-oidc-id"}}, "dist": {"attestations": {
            "url": URL, "provenance": {"predicateType": VERIFY.SLSA_V1}}}}
    statement = {"_type": "https://in-toto.io/Statement/v1", "predicateType": VERIFY.SLSA_V1,
        "subject": [{"name": "pkg:npm/%40erzenx/piko@0.7.1", "digest": {"sha512": SHA512}}],
        "predicate": {"buildDefinition": {"externalParameters": {"workflow": {
            "repository": VERIFY.REPOSITORY, "path": ".github/workflows/release.yml", "ref": "refs/tags/v0.7.1"}},
            "resolvedDependencies": [{"uri": "git+" + VERIFY.REPOSITORY + "@refs/tags/v0.7.1",
                                      "digest": {"gitCommit": COMMIT}}]}}}
    return manifest, statement


class Reader:
    def __init__(self, statement):
        self.document = {"attestations": [{"predicateType": VERIFY.SLSA_V1, "bundle": {
            "dsseEnvelope": {"payloadType": "application/vnd.in-toto+json",
                "payload": base64.b64encode(json.dumps(statement).encode()).decode()}}}]}
        self.urls = []

    def json(self, url):
        self.urls.append(url)
        return self.document


class ApprovalProvenanceTests(unittest.TestCase):
    def validate(self, manifest=None, statement=None, source=COMMIT):
        good_manifest, good_statement = fixtures()
        return VERIFY.approval_provenance(manifest or good_manifest, NAME, VERSION, SHA512,
                                         Reader(statement or good_statement), source)

    def test_valid_approval_records_explicit_claim_validation_limit(self):
        manifest, statement = fixtures()
        reader = Reader(statement)
        result = VERIFY.approval_provenance(manifest, NAME, VERSION, SHA512, reader)
        self.assertEqual(reader.urls, [URL])
        self.assertEqual(result["approver"], "erzenx")
        self.assertEqual(result["provenance"]["sourceCommit"], COMMIT)
        self.assertIn("no cryptographic Sigstore", result["provenance"]["validation"])

    def test_missing_blank_or_unstructured_approver_rejected_before_fetch(self):
        for approver in (None, {}, {"name": ""}, {"name": "  "}, {"name": 42}, "erzenx"):
            with self.subTest(approver=approver):
                manifest, statement = fixtures()
                manifest["_npmUser"]["approver"] = approver
                reader = Reader(statement)
                with self.assertRaisesRegex(ValueError, "staged approver"):
                    VERIFY.approval_provenance(manifest, NAME, VERSION, SHA512, reader)
                self.assertEqual(reader.urls, [])

    def test_publisher_identity_and_oidc_configuration_required(self):
        for publisher in ({}, {"id": "gitlab", "oidcConfigId": "x"}, {"id": "github"},
                          {"id": "github", "oidcConfigId": "  "}):
            manifest, _ = fixtures()
            manifest["_npmUser"]["trustedPublisher"] = publisher
            with self.subTest(publisher=publisher), self.assertRaisesRegex(ValueError, "trusted publisher"):
                self.validate(manifest=manifest)

    def test_attestation_url_bound_to_exact_registry_package_version(self):
        for url in (URL.replace("piko@", "other@"), URL.replace("0.7.1", "0.7.0"),
                    URL + "?other=1", URL.replace("registry.npmjs.org", "example.com")):
            manifest, _ = fixtures()
            manifest["dist"]["attestations"]["url"] = url
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.validate(manifest=manifest)

    def test_wrong_subject_identity_or_actual_tarball_digest_rejected(self):
        for key, value in (("digest", {"sha512": "c" * 128}), ("name", "pkg:npm/%40erzenx/other@0.7.1")):
            _, statement = fixtures()
            statement["subject"][0][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "subject SHA-512"):
                self.validate(statement=statement)

    def test_wrong_release_workflow_repository_and_tag_rejected(self):
        for key, value in (("repository", "https://github.com/other/piko"),
                           ("path", ".github/workflows/unsafe.yml"), ("ref", "refs/heads/main")):
            _, statement = fixtures()
            statement["predicate"]["buildDefinition"]["externalParameters"]["workflow"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "workflow/repository/ref"):
                self.validate(statement=statement)

    def test_wrong_source_commit_or_dependency_rejected(self):
        _, statement = fixtures()
        statement["predicate"]["buildDefinition"]["resolvedDependencies"][0]["digest"]["gitCommit"] = "c" * 40
        with self.assertRaisesRegex(ValueError, "source commit mismatch"):
            self.validate(statement=statement)
        statement["predicate"]["buildDefinition"]["resolvedDependencies"][0]["uri"] = "git+https://github.com/other/piko@refs/tags/v0.7.1"
        with self.assertRaisesRegex(ValueError, "bound release source"):
            self.validate(statement=statement)

    def test_first_package_source_anchors_remaining_packages(self):
        first = self.validate(source=None)
        _, second = fixtures()
        second["predicate"]["buildDefinition"]["resolvedDependencies"][0]["digest"]["gitCommit"] = "d" * 40
        with self.assertRaisesRegex(ValueError, "source commit mismatch"):
            self.validate(statement=second, source=first["provenance"]["sourceCommit"])

    def test_slsa_type_and_decoded_statement_not_just_metadata_required(self):
        manifest, statement = fixtures()
        statement["predicateType"] = "https://slsa.dev/provenance/v0.2"
        with self.assertRaisesRegex(ValueError, "SLSA v1 statement"):
            self.validate(statement=statement)
        reader = Reader(fixtures()[1])
        reader.document["attestations"].append(copy.deepcopy(reader.document["attestations"][0]))
        with self.assertRaisesRegex(ValueError, "one SLSA v1"):
            VERIFY.approval_provenance(manifest, NAME, VERSION, SHA512, reader)
        reader.document["attestations"] = reader.document["attestations"][:1]
        reader.document["attestations"][0]["bundle"]["dsseEnvelope"]["payload"] = "not base64!"
        with self.assertRaises(ValueError):
            VERIFY.approval_provenance(manifest, NAME, VERSION, SHA512, reader)


if __name__ == "__main__":
    unittest.main()
