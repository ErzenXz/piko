"""Guard comparison source selection and native-TLS experiment failure classification."""
import importlib.util
from pathlib import Path
import tempfile
import tomllib
import unittest

spec = importlib.util.spec_from_file_location('size_evaluation', Path(__file__).with_name('size-evaluation.py'))
evaluation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evaluation)


def results(failed=(), ignored=()):
    return '\n'.join('test ' + name + ' ... ' + ('FAILED' if name in failed else 'ignored' if name in ignored else 'ok')
                     for name in sorted(evaluation.TLS_TESTS))


class EvaluationTests(unittest.TestCase):
    def test_revisions_reject_options_operators_and_shell_fragments(self):
        for ref in ['--upload-pack=evil', 'HEAD~1', 'a;ls', 'x\ny', '$(x)', '/tmp/a', '']:
            with self.assertRaises(ValueError):
                evaluation.validate_ref(ref)
        for ref in [evaluation.DEFAULT_BASELINE, 'main', 'v0.7.1', 'feature/smaller']:
            self.assertEqual(evaluation.validate_ref(ref), ref)

    def test_native_patch_is_exact_and_preserves_other_features(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'Cargo.toml'
            path.write_text('[dependencies]\nreqwest = {version="0.12", default-features=false, features=["rustls-tls", "json", "stream"]}\n')
            evaluation.patch_native_tls(path)
            deps = tomllib.loads(path.read_text())['dependencies']
            self.assertEqual(deps['native-tls'], '=0.2.18')
            self.assertEqual(deps['reqwest']['features'], ['native-tls', 'json', 'stream'])
            with self.assertRaises(ValueError):
                evaluation.patch_native_tls(path)

    def test_all_tests_must_pass_on_windows(self):
        self.assertEqual(evaluation.tls_results(results(), 0, 'win32-x64')['passed'], 8)
        with self.assertRaises(ValueError):
            evaluation.tls_results(results([evaluation.TLS_13_TEST]), 101, 'win32-x64')

    def test_only_actual_protocol_failure_is_accepted_on_mac(self):
        output = results([evaluation.TLS_13_TEST]) + '\ntrusted TLS 1.3 endpoint; do not silently downgrade: bad protocol version\n'
        parsed = evaluation.tls_results(output, 101, 'darwin-arm64')
        self.assertEqual(parsed['failed'], 1)
        self.assertEqual(parsed['passed'], 7)
        for invalid in [results([evaluation.TLS_13_TEST]), results(['https_request_timeout_is_bounded']), output.replace('bad protocol version', 'timed out')]:
            with self.assertRaises(ValueError):
                evaluation.tls_results(invalid, 101, 'darwin-arm64')

    def test_missing_duplicate_ignored_and_build_failures_are_errors(self):
        valid = results()
        for output, code in [(valid.rsplit('\n', 1)[0], 0), (valid + '\n' + valid.splitlines()[0], 0),
                             (results(ignored=[evaluation.TLS_13_TEST]), 0), (valid, 101), ('compiler failed', 101)]:
            with self.assertRaises(ValueError):
                evaluation.tls_results(output, code, 'darwin-arm64')


if __name__ == '__main__':
    unittest.main()
