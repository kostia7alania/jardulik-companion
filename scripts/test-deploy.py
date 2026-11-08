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
                                 "deploy/jardulik-companion-poster.service",
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

    def run_activation_fixture(self, *, no_start=False, split=False, main="active", poster="missing", fail="", missing_pid=False):
        script = (SOURCE / "deploy/install.sh").read_text()
        helpers = script.split("# BEGIN runtime unit helpers\n", 1)[1].split("# END runtime unit helpers", 1)[0]
        activation = script.split("# BEGIN controlled activation\n", 1)[1]
        base = self.base / ("activation-" + str(len(list(self.base.glob("activation-*")))))
        old = base / "releases" / ("a" * 40)
        new = base / "releases" / ("b" * 40)
        old.mkdir(parents=True)
        new.mkdir()
        for folder, label in ((old, "old"), (new, "new")):
            for binary in ("jardulik", "telegram-transfer"):
                (folder / binary).write_text(label + ":" + binary)
        (base / "current").symlink_to(old)
        unit_dir = base / "systemd"
        unit_dir.mkdir()
        main_name = "jardulik-companion.service"
        poster_name = "jardulik-companion-poster.service"
        units = [main_name, poster_name, "jardulik-companion-backup.service", "jardulik-companion-backup.timer"]
        old_files = {main_name: "old split serve-bot-parser\n" if split else "old all-in-one serve\n",
                     units[2]: "old backup-daily\n", units[3]: "old timer\n"}
        if split or poster != "missing":
            old_files[poster_name] = "old split serve-poster\n"
        for name, body in old_files.items():
            (unit_dir / name).write_text(body)
        states = {name: {"state": "inactive", "pid": "0", "enabled": "disabled"} for name in units}
        states[main_name].update(state=main, pid="123" if main == "active" else "0", enabled="enabled")
        states[poster_name].update(state="inactive" if poster == "missing" else poster,
                                  pid="124" if poster == "active" else "0", enabled="enabled" if split else "disabled")
        states[units[3]].update(state="active", enabled="enabled")
        state_file = base / "states.json"
        state_file.write_text(json.dumps(states))
        control_log = base / "control.jsonl"
        env = dict(self.env, DEPLOY_TEST_BASE=str(base), DEPLOY_TEST_STATE=str(state_file),
                   DEPLOY_TEST_CONTROL_LOG=str(control_log), DEPLOY_TEST_UNITS=str(unit_dir),
                   DEPLOY_TEST_FAIL=fail, DEPLOY_TEST_NO_START="1" if no_start else "0",
                   DEPLOY_TEST_MISSING_PID="1" if missing_pid else "0")
        self.write_mock("mv", """#!/usr/bin/env python3
import os, sys
paths = [arg for arg in sys.argv[1:] if not arg.startswith('-')]
os.replace(paths[0], paths[1])
""")
        self.write_mock("systemctl", """#!/usr/bin/env python3
import json, os, pathlib, sys
base = pathlib.Path(os.environ['DEPLOY_TEST_BASE'])
state_file = pathlib.Path(os.environ['DEPLOY_TEST_STATE'])
unit_dir = pathlib.Path(os.environ['DEPLOY_TEST_UNITS'])
data = json.loads(state_file.read_text())
args = sys.argv[1:]
current = (base / 'current').resolve().name
with pathlib.Path(os.environ['DEPLOY_TEST_CONTROL_LOG']).open('a') as f:
    f.write(json.dumps({'args':args,'current':current}) + '\\n')
op = args[0]
if op == 'show':
    name = args[-1]
    row = data[name]
    values = {'LoadState':'loaded' if (unit_dir/name).exists() else 'not-found',
              'ActiveState':row['state'],'MainPID':row['pid']}
    properties = [a.split('=',1)[1] for a in args if a.startswith('--property=')]
    for prop in properties:
        if prop=='MainPID' and values['LoadState']=='not-found' and os.environ['DEPLOY_TEST_MISSING_PID']=='1':continue
        print(values[prop] if '--value' in args else prop+'='+values[prop])
elif op == 'is-enabled':
    name = args[-1]
    print(data[name]['enabled'] if (unit_dir/name).exists() else 'not-found')
    if not (unit_dir/name).exists() or data[name]['enabled'] == 'disabled': sys.exit(1)
elif op == 'daemon-reload':
    if os.environ['DEPLOY_TEST_FAIL'] == 'after-switch' and current == 'b'*40:
        sys.exit(1)
elif op in ('stop','start','enable','disable'):
    if os.environ['DEPLOY_TEST_NO_START'] == '1': sys.exit(98)
    names = [a for a in args[1:] if not a.startswith('-')]
    if op=='enable' and os.environ['DEPLOY_TEST_FAIL']=='enable-timer' and current=='b'*40 and 'jardulik-companion-backup.timer' in names:
        data['jardulik-companion-backup.timer']['state']='failed';state_file.write_text(json.dumps(data));sys.exit(1)
    for name in names:
        if op == 'start':
            if not (unit_dir/name).exists(): sys.exit(97)
            if current == 'a'*40 and name.endswith('-poster.service') and 'old split' not in (unit_dir/name).read_text(): sys.exit(96)
            if os.environ['DEPLOY_TEST_FAIL'] == 'poster-start' and current == 'b'*40 and name.endswith('-poster.service'):
                data[name].update(state='failed',pid='0');state_file.write_text(json.dumps(data));sys.exit(1)
            data[name].update(state='active',pid='456')
        elif op == 'stop': data[name].update(state='inactive',pid='0')
        elif op == 'enable':
            data[name]['enabled']='enabled'
            if '--now' in args: data[name].update(state='active')
        else: data[name]['enabled']='disabled'
    state_file.write_text(json.dumps(data))
else: sys.exit(95)
""")
        self.write_mock("install", """#!/usr/bin/env python3
import json, os, pathlib, shutil, sys
args=sys.argv[1:];paths=[];directory=False;i=0
while i<len(args):
    if args[i] in ('-o','-g','-m'):i+=2;continue
    if args[i]=='-d':directory=True;i+=1;continue
    paths.append(pathlib.Path(args[i]));i+=1
if directory:
    for p in paths:p.mkdir(parents=True,exist_ok=True)
else:
    source,target=paths
    states=json.loads(pathlib.Path(os.environ['DEPLOY_TEST_STATE']).read_text())
    if target.parent==pathlib.Path(os.environ['DEPLOY_TEST_UNITS']):
        if any(states[n]['state']=='active' for n in ('jardulik-companion.service','jardulik-companion-poster.service')):sys.exit(94)
        if os.environ['DEPLOY_TEST_FAIL']=='unit-copy' and target.name.endswith('-poster.service'):
            print('fixture unit-copy failure',file=sys.stderr);sys.exit(1)
    shutil.copyfile(source,target)
""")
        self.write_mock("systemd-run", """#!/usr/bin/env python3
import json,os,pathlib,sys
with pathlib.Path(os.environ['DEPLOY_TEST_CONTROL_LOG']).open('a') as f:f.write(json.dumps({'args':['systemd-run']})+'\\n')
if os.environ['DEPLOY_TEST_NO_START']=='1':sys.exit(99)
""")
        for name in ("systemd-analyze", "sleep"):
            self.write_mock(name, "#!/bin/sh\nexit 0\n")
        setup = 'set -Eeuo pipefail\ndie(){ echo "$*" >&2; exit 1; }\nbase=$1; release_dir=$2; previous_target=$3; source_dir=$4; unit_dir=$5; revision=' + "b" * 40 + "\n"
        setup += "data_dir=/unused; env_file=/unused; runtime_units=(jardulik-companion.service jardulik-companion-poster.service)\n"
        setup += 'unit_names=("${runtime_units[@]}" jardulik-companion-backup.service jardulik-companion-backup.timer)\nno_start=' + ("true" if no_start else "false") + "\n"
        result = subprocess.run(["bash", "-c", setup + helpers + activation, "activate", str(base), str(new), str(old), str(self.repo), str(unit_dir)],
                                env=env,capture_output=True,text=True,timeout=15)
        calls = [json.loads(line) for line in control_log.read_text().splitlines()] if control_log.exists() else []
        return result,base,unit_dir,old_files,json.loads(state_file.read_text()),calls

    def test_split_migration_stops_old_role_before_switch_and_starts_two_roles(self):
        result,base,unit_dir,old_files,states,calls=self.run_activation_fixture()
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual((base/'current').resolve().name,'b'*40)
        self.assertEqual((base/'previous').resolve().name,'a'*40)
        self.assertIn('serve-bot-parser',(unit_dir/'jardulik-companion.service').read_text())
        self.assertIn('serve-poster',(unit_dir/'jardulik-companion-poster.service').read_text())
        self.assertEqual(states['jardulik-companion.service']['state'],'active')
        self.assertEqual(states['jardulik-companion-poster.service']['state'],'active')
        stops=[c for c in calls if c['args'][0]=='stop']
        self.assertTrue(stops)
        self.assertTrue(all(c['current']=='a'*40 for c in stops))
        self.assertEqual(len(list((base/'deployments').glob('*/units/jardulik-companion.service'))),1)

    def test_failed_split_migration_restores_legacy_or_split_units_and_binary_pair(self):
        for split in (False,True):
            for fail in ('poster-start','unit-copy','after-switch','enable-timer'):
                with self.subTest(old_split=split,failure=fail):
                    result,base,unit_dir,old_files,states,calls=self.run_activation_fixture(split=split,poster='active' if split else 'missing',fail=fail)
                    self.assertNotEqual(result.returncode,0)
                    if fail=='unit-copy':self.assertIn('fixture unit-copy failure',result.stderr)
                    if fail=='poster-start':self.assertTrue(any(c['args'][0]=='start' and c['current']=='b'*40 for c in calls),calls)
                    if fail=='after-switch':self.assertTrue(any(c['args'][0]=='daemon-reload' and c['current']=='b'*40 for c in calls),calls)
                    if fail=='enable-timer':self.assertTrue(any(c['args'][0]=='enable' and '--now' in c['args'] and c['current']=='b'*40 for c in calls),calls)
                    self.assertEqual((base/'current').resolve().name,'a'*40,result.stderr)
                    for name,body in old_files.items():self.assertEqual((unit_dir/name).read_text(),body)
                    if not split:self.assertFalse((unit_dir/'jardulik-companion-poster.service').exists())
                    for binary in ('jardulik','telegram-transfer'):self.assertEqual((base/'current'/binary).read_text(),'old:'+binary)
                    self.assertEqual(states['jardulik-companion.service']['state'],'active',result.stderr)
                    self.assertEqual(states['jardulik-companion.service']['enabled'],'enabled',result.stderr)
                    self.assertEqual(states['jardulik-companion-poster.service']['enabled'],'enabled' if split else 'disabled',result.stderr)
                    self.assertEqual(states['jardulik-companion-backup.timer']['enabled'],'enabled',result.stderr)
                    self.assertEqual(states['jardulik-companion-backup.timer']['state'],'active',result.stderr)
                    if split:self.assertEqual(states['jardulik-companion-poster.service']['state'],'active',result.stderr)
                    old_starts=[c for c in calls if c['args'][0]=='start' and c['current']=='a'*40 and any(name in c['args'] for name in ('jardulik-companion.service','jardulik-companion-poster.service'))]
                    self.assertEqual(len(old_starts),2 if split else 1,result.stderr)

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

    def test_no_start_requires_both_roles_stopped_and_never_starts_on_rollback(self):
        cases=(('inactive','missing',''),('inactive','inactive',''),('inactive','missing','after-switch'),
               ('inactive','inactive','after-switch'),('active','missing',''),('inactive','active',''),
               ('inactive','failed',''))
        for main,poster,fail in cases:
            with self.subTest(main=main,poster=poster,failure=fail):
                result,base,unit_dir,old_files,states,calls=self.run_activation_fixture(no_start=True,main=main,poster=poster,fail=fail)
                success=main=='inactive' and poster in ('missing','inactive') and not fail
                self.assertEqual(result.returncode==0,success,result.stderr)
                self.assertEqual((base/'current').resolve().name,('b' if success else 'a')*40)
                forbidden={'start','restart','stop','enable','disable','systemd-run'}
                self.assertFalse(any(c['args'][0] in forbidden for c in calls),calls)
                if not success:
                    for name,body in old_files.items():self.assertEqual((unit_dir/name).read_text(),body)
                    if poster=='missing':self.assertFalse((unit_dir/'jardulik-companion-poster.service').exists())
                else:
                    self.assertIn('both roles remain stopped',result.stdout)
                    self.assertEqual(states['jardulik-companion.service']['state'],'inactive')
                    self.assertEqual(states['jardulik-companion-poster.service']['state'],'inactive')

    def test_first_migration_accepts_missing_poster_without_mainpid_property(self):
        result,base,unit_dir,old_files,states,calls=self.run_activation_fixture(no_start=True,main='inactive',poster='missing',missing_pid=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual((base/'current').resolve().name,'b'*40)
        self.assertFalse(any(c['args'][0] in {'start','restart','stop','enable','disable','systemd-run'} for c in calls),calls)

    def test_units_are_independent_and_backup_only_queues(self):
        main=(SOURCE/'deploy/jardulik-companion.service').read_text()
        poster=(SOURCE/'deploy/jardulik-companion-poster.service').read_text()
        self.assertIn('current/jardulik serve-bot-parser\n',main)
        self.assertIn('current/jardulik serve-poster\n',poster)
        for text in (main,poster):
            self.assertNotIn('PartOf=',text)
            self.assertNotIn('Requires=',text)
            self.assertIn('MemoryMax=128M\n',text)
            self.assertIn('Environment=GOMEMLIMIT=64MiB\n',text)
        self.assertNotIn('jardulik-companion.service',poster)
        self.assertNotIn('jardulik-companion-poster.service',main)
        backup=(SOURCE/'deploy/jardulik-companion-backup.service').read_text()
        self.assertIn('current/jardulik backup-daily\n',backup)
        self.assertNotIn('serve',backup)

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
