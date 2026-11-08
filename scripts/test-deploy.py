#!/usr/bin/env python3
"""Exercise deployment refusal and packaging without contacting a server."""

import base64
import hashlib
import json
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
        helper = self.repo / "tools/telegram-transfer"
        helper.mkdir(parents=True)
        (helper / "go.mod").write_text("module example.invalid/transfer-fixture\n\ngo 1.26.10\n")
        (helper / "go.sum").write_text("")
        self.mockbin = self.base / "bin"
        self.mockbin.mkdir()
        self.log = self.base / "ssh.log"
        self.package = self.base / "captured.tar.gz"
        self.go_log = self.base / "go.log"
        self.env = dict(os.environ, PATH=str(self.mockbin) + os.pathsep + os.environ["PATH"],
                        DEPLOY_TEST_LOG=str(self.log), DEPLOY_TEST_PACKAGE=str(self.package),
                        DEPLOY_TEST_GO_LOG=str(self.go_log))
        self.write_mock("go", """#!/usr/bin/env bash
set -euo pipefail
printf '%s | %s\\n' "$PWD" "$*" >> "$DEPLOY_TEST_GO_LOG"
if [[ ${1:-} == build ]]; then
    if [[ $PWD == */telegram-transfer && ${DEPLOY_TEST_FAIL_HELPER_BUILD:-0} == 1 ]]; then exit 1; fi
    if [[ $PWD == */telegram-transfer && ${DEPLOY_TEST_DIRTY_DURING_BUILD:-0} == 1 ]]; then printf 'changed' > ../../changed-during-build.txt; fi
    while (( $# )); do
        if [[ $1 == -o ]]; then shift; printf 'test-binary:%s' "${PWD##*/}" > "$1"; break; fi
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
            self.assertEqual(archive.extractfile("jardulik").read(), b"test-binary:checkout")
            self.assertEqual(archive.extractfile("telegram-transfer").read(), b"test-binary:telegram-transfer")
        self.assertEqual(names, {"jardulik", "telegram-transfer", "deploy", "deploy/install.sh",
                                 "deploy/jardulik-companion.service",
                                 "deploy/jardulik-companion-backup.service",
                                 "deploy/jardulik-companion-backup.timer"})
        self.assertNotIn("fixture-only", result.stdout + result.stderr)
        self.assertEqual(len(self.log.read_text().splitlines()), 3)

    def test_installer_refuses_bad_artifact_metadata_before_host_changes(self):
        script = SOURCE / "deploy/install.sh"
        result = subprocess.run(["bash", str(script), "not-a-commit", str(self.base), "0" * 64, "0" * 64],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid commit SHA", result.stderr)
        result = subprocess.run(["bash", str(script), "0" * 40, str(self.base), "not-a-checksum", "0" * 64],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid binary SHA-256", result.stderr)

    def test_helper_is_tested_separately_and_build_failure_never_contacts_server(self):
        self.commit_and_publish_locally()
        self.env["DEPLOY_TEST_FAIL_HELPER_BUILD"] = "1"
        result = self.run_deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.log.exists())
        calls = self.go_log.read_text().splitlines()
        helper = [line for line in calls if "/tools/telegram-transfer |" in line]
        self.assertTrue(any("test -race ./..." in line for line in helper))
        self.assertTrue(any("vet ./..." in line for line in helper))
        self.assertTrue(any("build " in line for line in helper))

    def test_checkout_change_during_helper_build_never_contacts_server(self):
        self.commit_and_publish_locally()
        self.env["DEPLOY_TEST_DIRTY_DURING_BUILD"] = "1"
        result = self.run_deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checkout changed while building", result.stderr)
        self.assertFalse(self.log.exists())

    def test_installer_rejects_missing_or_invalid_helper_before_host_mutations(self):
        script = SOURCE / "deploy/install.sh"
        result = subprocess.run(["bash", str(script), "0" * 40, str(self.base), "0" * 64, "bad"],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid helper SHA-256", result.stderr)
        (self.base / "jardulik").write_text("main")
        result = subprocess.run(["bash", str(script), "0" * 40, str(self.base), "0" * 64, "0" * 64],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("release helper is missing", result.stderr)
        (self.base / "telegram-transfer").symlink_to(self.base / "jardulik")
        result = subprocess.run(["bash", str(script), "0" * 40, str(self.base), "0" * 64, "0" * 64],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("release helper is missing", result.stderr)

    def test_installer_checks_both_source_checksums(self):
        script = (SOURCE / "deploy/install.sh").read_text()
        checks = "\n".join(line for line in script.splitlines()
                           if "| sha256sum --check --status || die" in line and '"$source_dir/' in line)
        self.assertEqual(len(checks.splitlines()), 2)
        self.write_mock("sha256sum", """#!/usr/bin/env python3
import hashlib, pathlib, sys
for line in sys.stdin:
    expected, path = line.strip().split(None, 1)
    if hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest() != expected:
        raise SystemExit(1)
""")
        main = self.base / "jardulik"
        helper = self.base / "telegram-transfer"
        main.write_bytes(b"main")
        helper.write_bytes(b"helper")
        prefix = "set -euo pipefail\ndie() { echo \"$*\" >&2; exit 1; }\n"
        setup = "source_dir=$1; binary_sha=$2; helper_sha=$3\n"
        good_main = hashlib.sha256(main.read_bytes()).hexdigest()
        good_helper = hashlib.sha256(helper.read_bytes()).hexdigest()
        for checksum, success in [(good_helper, True), ("0" * 64, False)]:
            result = subprocess.run(["bash", "-c", prefix + setup + checks,
                                     "checks", str(self.base), good_main, checksum],
                                    env=self.env, capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, success, result.stderr)
            if not success:
                self.assertIn("helper checksum mismatch", result.stderr)

    def test_atomic_activation_and_failed_start_restore_complete_binary_pair(self):
        script = (SOURCE / "deploy/install.sh").read_text()
        activation = script.split("switched=false\n", 1)[1]
        self.write_mock("mv", """#!/usr/bin/env python3
import os, sys
paths = [arg for arg in sys.argv[1:] if not arg.startswith('-')]
os.replace(paths[0], paths[1])
""")
        self.write_mock("systemctl", """#!/usr/bin/env python3
import os, pathlib, sys
if sys.argv[1] == 'restart':
    path = pathlib.Path(os.environ['DEPLOY_TEST_RESTART_COUNT'])
    count = int(path.read_text()) + 1 if path.exists() else 1
    path.write_text(str(count))
    if count == 1 and os.environ.get('DEPLOY_TEST_FAIL_RESTART') == '1':
        raise SystemExit(1)
""")
        for name in ("systemd-run", "systemd-analyze", "sleep"):
            self.write_mock(name, "#!/bin/sh\nexit 0\n")
        for fail in (False, True):
            with self.subTest(failed_activation=fail):
                base = self.base / ("rollback" if fail else "activation")
                old = base / "releases" / ("a" * 40)
                new = base / "releases" / ("b" * 40)
                old.mkdir(parents=True)
                new.mkdir()
                for folder, label in ((old, "old"), (new, "new")):
                    for binary in ("jardulik", "telegram-transfer"):
                        (folder / binary).write_text(label + ":" + binary)
                (base / "current").symlink_to(old)
                counter = base / "restart-count"
                env = dict(self.env, DEPLOY_TEST_RESTART_COUNT=str(counter),
                           DEPLOY_TEST_FAIL_RESTART="1" if fail else "0")
                setup = "set -Eeuo pipefail\nbase=$1; release_dir=$2; previous_target=$3; source_dir=$4; revision=" + "b" * 40 + "\n"
                setup += "data_dir=/unused; env_file=/unused; unit_names=(jardulik-companion.service)\nno_start=false\nswitched=false\n"
                result = subprocess.run(["bash", "-c", setup + activation,
                                         "activate", str(base), str(new), str(old), str(self.repo)],
                                        env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode == 0, not fail, result.stderr)
                expected = old if fail else new
                self.assertEqual((base / "current").resolve(), expected.resolve())
                for binary in ("jardulik", "telegram-transfer"):
                    self.assertEqual((base / "current" / binary).read_text(),
                                     ("old:" if fail else "new:") + binary)
                self.assertEqual(counter.read_text(), "2" if fail else "1")
                if not fail:
                    self.assertEqual((base / "previous").resolve(), old.resolve())

    def test_no_start_mode_reaches_remote_installer_and_rejects_unknown_mode(self):
        self.commit_and_publish_locally()
        result = self.run_deploy("--no-start", "sg-1gb")
        self.assertEqual(result.returncode, 0, result.stderr)
        remote = [line for line in self.log.read_text().splitlines() if "bash -s --" in line]
        self.assertEqual(len(remote), 1)
        self.assertTrue(remote[0].endswith(" true"), remote)
        result = subprocess.run(["bash", str(SOURCE / "deploy/install.sh"), "0" * 40,
                                 str(self.base), "0" * 64, "0" * 64, "--unknown-mode"],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid installation mode", result.stderr)

    def test_no_start_requires_inactive_and_rolls_back_without_starting_units(self):
        script = (SOURCE / "deploy/install.sh").read_text()
        guard = "require_inactive() {\n" + script.split("require_inactive() {\n", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        activation = script.split("switched=false\n", 1)[1]
        self.write_mock("mv", """#!/usr/bin/env python3
import os, sys
paths = [arg for arg in sys.argv[1:] if not arg.startswith('-')]
os.replace(paths[0], paths[1])
""")
        self.write_mock("systemctl", """#!/usr/bin/env python3
import os, pathlib, sys
with pathlib.Path(os.environ['DEPLOY_TEST_CONTROL_LOG']).open('a') as f:
    f.write(' '.join(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'show':
    print(os.environ['DEPLOY_TEST_SERVICE_STATE'])
else:
    raise SystemExit(99)
""")
        self.write_mock("systemd-analyze", """#!/bin/sh
if [ "$DEPLOY_TEST_FAIL_VERIFY" = 1 ]; then exit 1; fi
""")
        for name in ("systemd-run", "sleep"):
            self.write_mock(name, "#!/bin/sh\nexit 99\n")
        cases = (("inactive", False), ("inactive", True), ("active", False),
                 ("activating", False), ("failed", False))
        for index, (state, fail_verify) in enumerate(cases):
            with self.subTest(service_state=state, failed_verification=fail_verify):
                base = self.base / ("no-start-" + str(index))
                old = base / "releases" / ("a" * 40)
                new = base / "releases" / ("b" * 40)
                old.mkdir(parents=True)
                new.mkdir()
                for folder, label in ((old, "old"), (new, "new")):
                    for binary in ("jardulik", "telegram-transfer"):
                        (folder / binary).write_text(label + ":" + binary)
                (base / "current").symlink_to(old)
                control_log = base / "control.log"
                env = dict(self.env, DEPLOY_TEST_CONTROL_LOG=str(control_log),
                           DEPLOY_TEST_SERVICE_STATE=state,
                           DEPLOY_TEST_FAIL_VERIFY="1" if fail_verify else "0")
                setup = "set -Eeuo pipefail\nbase=$1; release_dir=$2; previous_target=$3; source_dir=$4; revision=" + "b" * 40 + "\n"
                setup += "data_dir=/unused; env_file=/unused; unit_names=(jardulik-companion.service)\nno_start=true\nswitched=false\n"
                result = subprocess.run(["bash", "-c", setup + guard + activation,
                                         "install-stopped", str(base), str(new), str(old), str(self.repo)],
                                        env=env, capture_output=True, text=True, timeout=10)
                success = state == "inactive" and not fail_verify
                self.assertEqual(result.returncode == 0, success, result.stderr)
                expected, label = (new, "new") if success else (old, "old")
                self.assertEqual((base / "current").resolve(), expected.resolve())
                for binary in ("jardulik", "telegram-transfer"):
                    self.assertEqual((base / "current" / binary).read_text(), label + ":" + binary)
                calls = control_log.read_text().splitlines()
                self.assertTrue(calls)
                self.assertTrue(all(line.startswith("show ") for line in calls), calls)
                self.assertEqual(len(calls), 2 if success else 1)
                if success:
                    self.assertIn("without starting", result.stdout)
                    self.assertEqual((base / "previous").resolve(), old.resolve())
                if state != "inactive":
                    self.assertIn("must already be inactive", result.stderr)

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
