#!/usr/bin/env python3
"""Exercise deployment refusal and packaging without contacting a server."""

import base64
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1]


class DeploymentSafetyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="jardulik-deploy-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.repo = self.base / "checkout"
        self.repo.mkdir()
        shutil.copytree(SOURCE / "scripts", self.repo / "scripts")
        shutil.copytree(SOURCE / "deploy", self.repo / "deploy")
        self.mockbin = self.base / "bin"
        self.mockbin.mkdir()
        self.log = self.base / "ssh.log"
        self.package = self.base / "captured.tar.gz"
        self.env = dict(os.environ, PATH=str(self.mockbin) + os.pathsep + os.environ["PATH"],
                        DEPLOY_TEST_LOG=str(self.log), DEPLOY_TEST_PACKAGE=str(self.package))
        self.write_mock("go", """#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == build ]]; then
    while (( $# )); do
        if [[ $1 == -o ]]; then shift; printf 'test-binary' > "$1"; break; fi
        shift
    done
fi
""")
        self.write_mock("ssh", """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$DEPLOY_TEST_LOG"
case "$*" in
    *'mktemp -d'*) printf '%s\\n' "${DEPLOY_TEST_STAGE:-/tmp/jardulik-deploy.ABC123}" ;;
    *'bash -s'*) cat > /dev/null ;;
esac
""")
        self.write_mock("scp", """#!/usr/bin/env bash
set -euo pipefail
while (( $# > 2 )); do shift; done
cp "$1" "$DEPLOY_TEST_PACKAGE"
""")
        self.git("init", "-b", "main")

    def write_mock(self, name, content):
        path = self.mockbin / name
        path.write_text(content)
        path.chmod(0o755)

    def git(self, *args):
        return subprocess.run(["git", "-c", "user.name=Deploy Test", "-c", "user.email=deploy-test@example.invalid", *args],
                              cwd=self.repo, check=True, capture_output=True, text=True)

    def commit_and_publish_locally(self):
        (self.repo / ".env").write_text("DO_NOT_PACKAGE=fixture-only\n")
        self.git("add", ".")
        self.git("commit", "-m", "Test release")
        remote = self.base / "remote.git"
        subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
        self.git("remote", "add", "origin", str(remote))
        self.git("push", "origin", "main")

    def run_deploy(self, *args):
        return subprocess.run(["bash", str(self.repo / "scripts/deploy.sh"), *args],
                              cwd=self.repo, env=self.env, capture_output=True, text=True, timeout=30)

    def test_refuses_missing_commit_and_unsafe_host_without_ssh(self):
        result = self.run_deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("create and commit", result.stderr)
        result = self.run_deploy("sg-1gb; unexpected-command")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid SSH host", result.stderr)
        self.assertFalse(self.log.exists())

    def test_refuses_dirty_checkout_without_ssh(self):
        self.commit_and_publish_locally()
        (self.repo / "uncommitted.go").write_text("package main\n")
        result = self.run_deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("commit all changes", result.stderr)
        self.assertFalse(self.log.exists())

    def test_rejects_untrusted_remote_path_before_cleanup(self):
        self.commit_and_publish_locally()
        self.env["DEPLOY_TEST_STAGE"] = "/tmp/unexpected'; do-not-run-this"
        result = self.run_deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected staging path", result.stderr)
        self.assertEqual(len(self.log.read_text().splitlines()), 1)
        self.assertFalse(self.package.exists())

    def test_package_contains_only_binary_and_explicit_deployment_files(self):
        self.commit_and_publish_locally()
        result = self.run_deploy()
        self.assertEqual(result.returncode, 0, result.stderr)
        with tarfile.open(self.package) as archive:
            names = set(archive.getnames())
            self.assertEqual(archive.extractfile("jardulik").read(), b"test-binary")
        self.assertEqual(names, {"jardulik", "deploy", "deploy/install.sh",
                                 "deploy/jardulik-companion.service",
                                 "deploy/jardulik-companion-backup.service",
                                 "deploy/jardulik-companion-backup.timer"})
        self.assertNotIn("fixture-only", result.stdout + result.stderr)
        self.assertEqual(len(self.log.read_text().splitlines()), 3)

    def test_installer_refuses_bad_artifact_metadata_before_host_changes(self):
        script = SOURCE / "deploy/install.sh"
        result = subprocess.run(["bash", str(script), "not-a-commit", str(self.base), "0" * 64],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid commit SHA", result.stderr)
        result = subprocess.run(["bash", str(script), "0" * 40, str(self.base), "not-a-checksum"],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid binary SHA-256", result.stderr)

    def test_environment_gate_rejects_unsafe_values_without_disclosing_secrets(self):
        script = (SOURCE / "deploy/install.sh").read_text()
        program = script.split("python3 - \"$env_file\" <<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        key = base64.b64encode(bytes(range(32))).decode()
        values = {"APP_DATA_DIR": "/var/lib/jardulik-companion", "SESSION_KEY": key,
                  "BACKUP_KEY": key, "OWNER_TELEGRAM_ID": "12345", "JARDULIK_BOT_ID": "67890",
                  "TELEGRAM_BOT_TOKEN": "123456789:fixture_token_not_real",
                  "BACKUP_CHAT_ID": "-10012345", "BACKUP_THREAD_ID": "2", "ALLOW_REACTIONS": "false"}
        env_path = self.base / "test.env"

        def validate(candidate):
            env_path.write_text("".join(f"{key}={value}\n" for key, value in candidate.items()))
            result = subprocess.run([sys.executable, "-c", program, str(env_path)],
                                    capture_output=True, text=True, timeout=10)
            self.assertNotIn(values["SESSION_KEY"], result.stdout + result.stderr)
            self.assertNotIn(values["TELEGRAM_BOT_TOKEN"], result.stdout + result.stderr)
            return result

        result = validate(values)
        self.assertEqual(result.returncode, 0, result.stderr)
        for changes in ({"ALLOW_REACTIONS": "true"}, {"SESSION_KEY": "bad-key"},
                        {"JARDULIK_BOT_ID": "0"}, {"BACKUP_CHAT_ID": "12345"},
                        {"MIN_FREE_BYTES": "1000"}):
            with self.subTest(changes=changes):
                result = validate(dict(values, **changes))
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
