"""The job sandbox in the queue engine: host setting, request fields, resolver, launcher text and failure handling,
fleet carry-through. No host is contacted; run.sh is executed in a local bash against a stand-in helper."""
import base64
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_cq import Base, BASH, HAVE_BASH
import cq
import fleet_queue as fq
import multi_host
import queue_generations

# the job command line of run.sh as it was before the sandbox existed
PLAIN_INNER = '''inner='date +%s > "$D/started"; exec timeout -k 30 "$TIMEOUT_S" bash "$D/cmd.sh"\''''
BOXED_INNER = ('''inner='date +%s > "$D/started"; exec /usr/local/libexec/baton/jobbox{net} --dir "$D" '''
               '''${{CQ_BOX_RW:+--rw "$CQ_BOX_RW"}} -- timeout -k 30 "$TIMEOUT_S" bash "$D/cmd.sh"\'''')
GOFLAGS_LINE = 'export GOFLAGS="${GOFLAGS:+$GOFLAGS }-buildvcs=false"'
INVALID = 'host setting "sandbox" must be off, optional or required'
NO_BOX_SETTING ="this host has no job sandbox (host setting \"sandbox\" is off)"
REQUIRED = "this host requires the job sandbox"


def unix(path):
    return str(path).replace("\\", "/")


class ResolverTests(unittest.TestCase):
    def test_every_host_mode_and_request(self):
        table = [  # (mode, sandbox, network) -> (boxed, network) or the refusal
            ("off", None, False, (False, False)), ("off", False, False, (False, False)),
            ("off", None, True, (False, False)), ("off", False, True, (False, False)),  # network ignored when not boxed
            ("off", True, False, NO_BOX_SETTING), ("off", True, True, NO_BOX_SETTING),
            (None, None, False, (False, False)), (None, True, False, NO_BOX_SETTING),  # absent setting means off
            ("optional", None, False, (False, False)), ("optional", False, False, (False, False)),
            ("optional", None, True, (False, False)), ("optional", False, True, (False, False)),
            ("optional", True, False, (True, False)), ("optional", True, True, (True, True)),
            ("required", None, False, (True, False)), ("required", None, True, (True, True)),
            ("required", True, False, (True, False)), ("required", True, True, (True, True)),
            ("required", False, False, REQUIRED), ("required", False, True, REQUIRED),
        ]
        for mode, sandbox, network, expected in table:
            with self.subTest(mode=mode, sandbox=sandbox, network=network):
                binding = {} if mode is None else {"sandbox": mode}
                job = {"sandbox": sandbox, "network": network}
                if isinstance(expected, str):
                    with self.assertRaises(cq.SandboxError) as caught:
                        cq.sandbox_plan(job, binding)
                    self.assertEqual(str(caught.exception), expected)
                    self.assertIsInstance(caught.exception, cq.CqError)
                else:
                    self.assertEqual(cq.sandbox_plan(job, binding), expected)

    def test_a_job_without_the_fields_and_no_binding(self):
        self.assertEqual(cq.sandbox_plan({}, None), (False, False))
        self.assertEqual(cq.sandbox_plan({}, {"sandbox": "required"}), (True, False))

    def test_request_types(self):
        self.assertEqual(cq.sandbox_request({}), (None, False))
        self.assertEqual(cq.sandbox_request({"sandbox": None, "network": None}), (None, False))
        self.assertEqual(cq.sandbox_request({"sandbox": False, "network": True}), (False, True))
        for bad in ("yes", 1, 0, [], {}):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(cq.CqError, "^sandbox must be true or false$"):
                    cq.sandbox_request({"sandbox": bad})
                with self.assertRaisesRegex(cq.CqError, "^network must be true or false$"):
                    cq.sandbox_request({"network": bad})


class HostSettingTests(Base):
    def test_valid_values_only(self):
        check = cq.HOST_CONF_VALID["sandbox"]
        for value in ("off", "optional", "required"):
            self.assertTrue(check(value))
        for value in ("on", "Required", "", None, True, 1, [], ["off"]):
            self.assertFalse(cq._valid(check, value), value)

    def test_host_conf_keeps_a_valid_value_and_drops_the_rest(self):
        state = os.path.join(self.tmp, "conf")
        for value, kept in (("required", "required"), ("optional", "optional"), (" off ", "off")):
            cq.write_json(os.path.join(state, "host.conf.json"), {"sandbox": value})
            self.assertEqual(cq.host_conf(state).get("sandbox"), kept, value)

    def test_a_mistyped_setting_is_kept_as_invalid_so_that_it_fails_closed(self):
        state = os.path.join(self.tmp, "conf")
        for value in ("Required", "OFF", "on", "", True, False, 1, None, [], ["off"], "invalid"):
            cq.write_json(os.path.join(state, "host.conf.json"), {"sandbox": value, "slots": 4})
            conf = cq.host_conf(state)
            self.assertEqual(conf.get("sandbox"), "invalid", repr(value))
            self.assertEqual(conf.get("slots"), 4)
        # every other invalid key is still dropped, and an absent setting stays absent
        cq.write_json(os.path.join(state, "host.conf.json"), {"slots": "many", "go_trimpath": "yes", "tmp_root": "relative"})
        self.assertEqual(cq.host_conf(state), {})

    def test_an_invalid_setting_refuses_every_job_on_the_host(self):
        for asked in (None, False, True):
            with self.subTest(asked=asked):
                with self.assertRaisesRegex(cq.SandboxError, '^host setting "sandbox" must be off, optional or required$'):
                    cq.sandbox_plan({"sandbox": asked}, {"sandbox": "invalid"})
        with mock.patch.object(cq, "_CONF", {"sandbox": "invalid"}):
            self.assertEqual(cq.host_binding()["sandbox"], "invalid")
            with self.assertRaisesRegex(cq.SandboxError, "must be off, optional or required"):
                self.submit()  # not even a job that asked for nothing
            with self.assertRaisesRegex(cq.SandboxError, "must be off, optional or required"):
                self.submit(sandbox=False)
        self.assertEqual(cq.all_jobs(self.ctx), [])
        self.assertEqual(cq.stricter_sandbox("off", "invalid"), "invalid")
        self.assertEqual(cq.stricter_sandbox("required", "optional", None), "required")
        self.assertEqual(cq.stricter_sandbox("off", "typo"), "invalid")
        self.assertEqual(cq.stricter_sandbox(None), "off")

    def test_binding_of_the_default_host(self):
        self.assertEqual(cq.host_binding()["sandbox"], "off")
        with mock.patch.object(cq, "_CONF", {"sandbox": "required"}):
            self.assertEqual(cq.host_binding()["sandbox"], "required")

    def test_a_heavy_host_job_that_names_no_host_is_accepted_as_a_fleet_job_on_ember(self):
        cq.write_json(self.ctx.p("host.conf.json"), {"name": "ember", "address": "build@ember", "slots": 8})
        self.addCleanup(cq.refresh_host_config, os.environ["CQ_STATE"])
        who = {"root": self.repo, "session": "sess-A"}
        result = cq.execute(self.ctx, "submit", dict(who, command="FOO=1 bash build.sh --all", slots=2, est_minutes=7,
                                                     requires=["no-such-tool"]))
        self.assertTrue(result["ok"], result)
        self.assertTrue(result["job"].startswith("cq-v2--fleet-"), result["job"])
        self.assertEqual(result["note"], "fleet job accepted; this where: host job runs as a contained fleet job on ember: "
                                         "CPU held to 2 slots, memory capped at 8 GB (state memory_max_gb to change it)")
        job = cq.load_job(queue_generations.context(cq, self.ctx, 2), result["job"])
        self.assertEqual((job["candidates"], job["slots"], job["memory_max_gb"], job["requires"], job["label"], job["est_minutes"]),
                         (["ember"], 2, 8.0, [], "bash build.sh --all", 7))
        self.assertEqual(cq.all_jobs(self.ctx), [])   # nothing went to the ordinary queue
        stated = cq.execute(self.ctx, "submit", dict(who, command="bash build.sh", where="host", slots=2, memory_max_gb=3))
        self.assertIn("memory capped at 3 GB", stated["note"])
        # larger than the fleet pool (8 slots less 2 for foreground work): an ordinary job, as it always was
        big = cq.execute(self.ctx, "submit", dict(who, command="bash build.sh", slots=7))
        self.assertTrue(big["ok"], big)
        self.assertFalse(big["job"].startswith("cq-v2--"), big["job"])
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], [big["job"]])
        # a refusal by the fleet backend says what happened to the request
        with self.assertRaisesRegex(cq.CqError, r"resource_key.*\(a heavy where: host job that names no host now runs as a contained"):
            cq.execute(self.ctx, "submit", dict(who, command="bash build.sh", slots=2, resource_key=5))

    def test_binding_of_an_explicitly_selected_host_shard(self):
        cq.write_json(self.ctx.p("host.conf.json"), {"name": "ember", "address": "build@ember", "slots": 4})
        shard = cq.Ctx(state=self.ctx.p("hosts", "hearth"), now=self.ctx.now, ssh=self.ctx.ssh,
                       run_remoterun=self.ctx.run_remoterun, spawn=self.ctx.spawn, laptop=self.laptop, sync=True)
        cq.write_json(shard.p("host.conf.json"), {"host_id": "hearth", "name": "hearth", "address": "worker@lan",
                                                  "slots": 8, "machine_id": "a" * 32, "sandbox": "optional"})
        shard.host_id = "hearth"
        cq.write_json(self.ctx.p("hosts.json"), {"version": 1, "hosts": {"hearth": {"state": "hosts/hearth", "enabled": True}}})
        self.addCleanup(cq.refresh_host_config, os.environ["CQ_STATE"])
        result = cq.execute(self.ctx, "submit", {"command": "echo a", "root": self.repo, "session": "sess-A", "host_id": "hearth",
                                                 "where": "host", "sandbox": True, "network": True})
        self.assertTrue(result["ok"], result)
        cq._execute_single(shard, "dispatch", {"job": result["job"], "where": "host"})
        job = multi_host.load_job(cq, self.ctx, result["job"])
        self.assertEqual(job["host_config"]["sandbox"], "optional")
        self.assertIn("jobbox --net --dir", cq.make_launcher(job))
        # the ember host has no setting: the same request is refused there
        cq.refresh_host_config(self.ctx.state)
        with self.assertRaisesRegex(cq.CqError, "no job sandbox"):
            self.submit(sandbox=True)


class SubmitTests(Base):
    def setUp(self):
        super().setUp()
        self.conf = {}
        patch = mock.patch.object(cq, "_CONF", self.conf)
        patch.start()
        self.addCleanup(patch.stop)

    def mode(self, mode):
        self.conf.clear()
        if mode:
            self.conf["sandbox"] = mode

    def refused(self, message, **kw):
        before = cq.all_jobs(self.ctx)
        with self.assertRaisesRegex(cq.CqError, re.escape(message)):
            self.submit(**kw)
        self.assertEqual(cq.all_jobs(self.ctx), before)  # refused before the snapshot: nothing stored

    def test_default_record(self):
        job = self.job(self.submit())
        self.assertIsNone(job["sandbox"])
        self.assertIs(job["network"], False)

    def test_record_holds_the_request_as_given(self):
        self.mode("optional")
        job = self.job(self.submit(sandbox=True, network=True))
        self.assertIs(job["sandbox"], True)
        self.assertIs(job["network"], True)
        job = self.job(self.submit(sandbox=False))
        self.assertIs(job["sandbox"], False)
        self.assertIs(job["network"], False)

    def test_type_errors_are_refused(self):
        self.mode("optional")
        for kw, message in (({"sandbox": "yes"}, "sandbox must be true or false"), ({"sandbox": 1}, "sandbox must be true or false"),
                            ({"network": "no"}, "network must be true or false"), ({"network": 0}, "network must be true or false")):
            with self.subTest(kw=kw):
                self.refused(message, **kw)

    def test_host_modes(self):
        self.mode(None)
        self.refused(NO_BOX_SETTING, sandbox=True)
        self.assertTrue(self.submit(sandbox=False))
        self.assertTrue(self.submit(network=True))  # no effect, not an error
        self.mode("off")
        self.refused(NO_BOX_SETTING, sandbox=True, network=True)
        self.mode("optional")
        self.assertTrue(self.submit(sandbox=True))
        self.assertTrue(self.submit())
        self.mode("required")
        self.refused(REQUIRED, sandbox=False)
        self.assertTrue(self.submit())
        self.assertTrue(self.submit(sandbox=True, network=True))

    def test_laptop_has_no_sandbox(self):
        self.mode("optional")
        self.refused("the job sandbox exists only on Linux hosts", sandbox=True, where="laptop", slots=0)
        self.assertTrue(self.submit(sandbox=False, where="laptop", slots=0))
        self.assertTrue(self.submit(where="laptop", slots=0))

    def test_any_with_sandbox_is_pinned_to_the_host(self):
        self.mode("optional")
        jid = self.submit(sandbox=True, where="any", slots=0)
        self.assertEqual(self.job(jid)["where"], "host")
        self.assertEqual(self.job(self.submit(where="any", slots=0))["where"], "any")  # nothing asked: placement unchanged
        self.mode(None)
        self.refused(NO_BOX_SETTING, sandbox=True, where="any", slots=0)

    def test_status_rows_show_the_fields_only_when_true(self):
        self.mode("optional")
        plain, boxed, net = (self.job(self.submit(**kw)) for kw in ({}, {"sandbox": True}, {"sandbox": True, "network": True}))
        row = cq.job_summary(plain, self.clock[0])
        self.assertNotIn("sandbox", row)
        self.assertNotIn("network", row)
        self.assertIs(cq.job_summary(boxed, self.clock[0])["sandbox"], True)
        self.assertNotIn("network", cq.job_summary(boxed, self.clock[0]))
        self.assertIs(cq.job_summary(net, self.clock[0])["network"], True)
        self.assertEqual(cq.job_summary(dict(plain, sandbox=False), self.clock[0]).get("sandbox"), None)

    def test_get_returns_the_stored_fields(self):
        self.mode("optional")
        jid = self.submit(sandbox=True, network=True)
        got = cq.cmd_get(self.ctx, {"job": jid, "full": True})["job"]
        self.assertIs(got["sandbox"], True)
        self.assertIs(got["network"], True)

    def test_split_parts_keep_the_request(self):
        self.mode("optional")
        jid = self.submit("echo {part}", sandbox=True, network=True, split={"max_parts": 2})
        self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host", "parts": 2})["ok"])
        parent = self.job(jid)
        for child in parent["children"]:
            self.assertEqual((self.job(child)["sandbox"], self.job(child)["network"]), (True, True))


class LauncherTests(Base):
    def make(self, host_config=None, **job):
        jid = self.submit(**{k: v for k, v in job.items() if k in ("sandbox", "network", "temp_storage", "memory_max_gb", "where", "host_id", "defer", "slots")} or {})
        record = self.job(jid)
        record["slots_eff"] = record["slots"]
        if host_config is not None:
            record["host_config"] = host_config
        return record

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, "_CONF", {"sandbox": "optional"})
        patch.start()
        self.addCleanup(patch.stop)

    def test_unboxed_launcher_is_what_it_was(self):
        for host_config, request in (({"sandbox": "off"}, {}), ({"sandbox": "optional"}, {}), ({"sandbox": "optional"}, {"sandbox": False}),
                                     ({"sandbox": "optional"}, {"network": True}), ({}, {}), (None, {})):
            with self.subTest(host_config=host_config, request=request):
                script = cq.make_launcher(self.make(host_config, **request))
                self.assertIn(PLAIN_INNER, script)
                for text in ("jobbox", "CQ_BOX_RW", "CQ_NO_JOBBOX", "--net", "--rw", "buildvcs"):
                    self.assertNotIn(text, script)
        # the template itself holds no trace of the box
        self.assertIn(PLAIN_INNER, cq.LAUNCHER)
        self.assertNotIn("jobbox", cq.LAUNCHER)

    def test_boxed_job_command_line(self):
        script = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True))
        self.assertIn(BOXED_INNER.format(net=""), script)
        self.assertNotIn(PLAIN_INNER, script)
        self.assertNotIn("--net", script)
        self.assertIn('export CQ_BOX_RW="${RT:-${CQ_RAM_TMP:-}}"\n' + GOFLAGS_LINE + '\ninner=', script)
        self.assertLess(script.index("CQ_BOX_RW="), script.index("inner="))
        # started is written outside the box; timeout runs inside it
        self.assertLess(script.index('date +%s > "$D/started"'), script.index("exec /usr/local/libexec/baton/jobbox"))
        self.assertLess(script.index("exec /usr/local/libexec/baton/jobbox"), script.index("timeout -k 30"))

    def test_network_flag(self):
        net = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True, network=True))
        self.assertIn(BOXED_INNER.format(net=" --net"), net)
        # network without a box is ignored
        self.assertNotIn("jobbox", cq.make_launcher(self.make({"sandbox": "optional"}, network=True)))

    def test_required_host_boxes_a_job_that_did_not_ask(self):
        script = cq.make_launcher(self.make({"sandbox": "required"}))
        self.assertIn(BOXED_INNER.format(net=""), script)

    def test_unbound_job_reads_the_current_host_setting(self):
        record = self.make(None, sandbox=True)  # no host_config yet
        self.assertIn("jobbox", cq.make_launcher(record))
        with mock.patch.object(cq, "_CONF", {}), self.assertRaisesRegex(cq.SandboxError, "no job sandbox"):
            cq.make_launcher(record)

    def test_a_late_binding_is_authoritative(self):
        record = self.make({"sandbox": "optional"}, sandbox=True)
        with mock.patch.object(cq, "_CONF", {}):  # the loaded host has no setting: the binding alone is the host's
            with self.assertRaises(cq.SandboxError) as caught:
                cq.make_launcher(dict(record, host_config={"sandbox": "off"}))
            self.assertEqual(str(caught.exception), NO_BOX_SETTING)
        record = self.make({"sandbox": "optional"}, sandbox=False)
        with self.assertRaises(cq.SandboxError) as caught:
            cq.make_launcher(dict(record, host_config={"sandbox": "required"}))
        self.assertEqual(str(caught.exception), REQUIRED)
        with self.assertRaisesRegex(cq.SandboxError, INVALID):
            cq.make_launcher(dict(record, host_config={"sandbox": "invalid"}))

    def test_a_frozen_binding_never_weakens_the_loaded_setting(self):
        asked = self.make({"sandbox": "off"})  # a job that did not ask for a box
        refusing = self.make({"sandbox": "off"}, sandbox=False)
        for binding, label in (({"sandbox": "off", "slots": 4}, "binding off"), ({"slots": 4}, "binding without the key"),
                               ({"sandbox": "optional"}, "binding optional"), ({"sandbox": "required"}, "binding required"),
                               ({}, "no binding")):
            with self.subTest(label), mock.patch.object(cq, "_CONF", {"sandbox": "required"}):
                self.assertIn(BOXED_INNER.format(net=""), cq.make_launcher(dict(asked, host_config=binding)))
                with self.assertRaises(cq.SandboxError) as caught:  # sandbox: false against a host now requiring it
                    cq.make_launcher(dict(refusing, host_config=binding))
                self.assertEqual(str(caught.exception), REQUIRED)
        # the binding's own setting still holds when the loaded one is looser
        for loaded in ({}, {"sandbox": "off"}, {"sandbox": "optional"}):
            with self.subTest(loaded=loaded), mock.patch.object(cq, "_CONF", loaded):
                self.assertIn(BOXED_INNER.format(net=""), cq.make_launcher(dict(asked, host_config={"sandbox": "required"})))
        # a mistyped loaded setting wins over any binding
        with mock.patch.object(cq, "_CONF", {"sandbox": "invalid"}):
            with self.assertRaisesRegex(cq.SandboxError, INVALID):
                cq.make_launcher(dict(asked, host_config={"sandbox": "off"}))

    def test_a_binding_for_another_machine_decides_alone(self):
        asked = self.make({"sandbox": "off"}, sandbox=True)
        other = {"machine_id": "b" * 32}
        with mock.patch.object(cq, "_CONF", {"sandbox": "required", "machine_id": "a" * 32}):
            with self.assertRaises(cq.SandboxError) as caught:  # binding off, the loaded host's requirement is not its own
                cq.make_launcher(dict(asked, host_config={**other, "sandbox": "off"}))
            self.assertEqual(str(caught.exception), NO_BOX_SETTING)
            self.assertNotIn("jobbox", cq.make_launcher(dict(self.make({"sandbox": "off"}), host_config={**other, "sandbox": "off"})))
            self.assertIn("jobbox", cq.make_launcher(dict(self.make({"sandbox": "off"}), host_config={**other, "sandbox": "required"})))
            # the same machine: the loaded setting applies
            self.assertIn("jobbox", cq.make_launcher(dict(self.make({"sandbox": "off"}), host_config={"machine_id": "a" * 32, "sandbox": "off"})))

    def test_launcher_template_mismatch_is_loud(self):
        record = self.make({"sandbox": "optional"}, sandbox=True)
        for target, damaged in (("exec timeout -k 30", "exec nice timeout -k 30"), ("inner='date +%s", "inner=' date +%s"),
                                ('mkdir -p "$Q"', 'mkdir  -p "$Q"')):
            with self.subTest(target=target), mock.patch.object(cq, "LAUNCHER", cq.LAUNCHER.replace(target, damaged)):
                with self.assertRaises(cq.CqError) as caught:
                    cq.make_launcher(record)
                self.assertEqual(str(caught.exception), "sandbox launcher template mismatch")
                self.assertNotIsInstance(caught.exception, cq.SandboxError)
        # an unboxed job does not touch those substitutions
        with mock.patch.object(cq, "LAUNCHER", cq.LAUNCHER.replace("exec timeout -k 30", "exec nice timeout -k 30")):
            self.assertIn("exec nice timeout", cq.make_launcher(self.make({"sandbox": "optional"})))

    def test_goflags_line_only_when_boxed(self):
        self.assertIn(GOFLAGS_LINE, cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True)))
        self.assertNotIn("GOFLAGS=\"${GOFLAGS", cq.make_launcher(self.make({"sandbox": "optional"})))
        with mock.patch.object(cq, "_CONF", {"sandbox": "required"}):  # not asked for, but the host requires it: boxed
            self.assertIn(GOFLAGS_LINE, cq.make_launcher(self.make(None)))

    def test_helper_check_only_for_a_boxed_job_and_before_anything_is_made(self):
        boxed = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True))
        plain = cq.make_launcher(self.make({"sandbox": "optional"}))
        check = '[ -f /usr/local/libexec/baton/jobbox ] && [ -x /usr/local/libexec/baton/jobbox ] || '
        self.assertIn(check, boxed)
        self.assertNotIn("CQ_NO_JOBBOX", plain)
        self.assertLess(boxed.index(check), boxed.index('mkdir -p "$Q"'))
        self.assertLess(boxed.index(check), boxed.index('mkdir "$D"'))
        self.assertIn("exit 79", boxed)

    def test_ram_launcher_keeps_its_service_and_gains_the_box(self):
        with mock.patch.object(cq, "RAM_JOBS", True):
            script = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True, temp_storage="ram", memory_max_gb=1))
        self.assertIn('cq-job" start', script)
        self.assertIn('export TMPDIR="${CQ_RAM_TMP:?RAM service required}"', script)
        self.assertIn(BOXED_INNER.format(net=""), script)
        self.assertNotIn("setsid nohup", script)
        # the service's check comes first, then the helper's, both before the queue directory is made
        self.assertLess(script.index('cq-job" check'), script.index("CQ_NO_JOBBOX"))
        self.assertLess(script.index("CQ_NO_JOBBOX"), script.index('mkdir -p "$Q"'))

    def test_deferred_launcher_keeps_its_marker_and_gains_the_box(self):
        jid = self.submit(where="host", host_id="ember", defer="overnight", memory_max_gb=2, sandbox=True, network=True)
        script = cq.make_launcher(dict(self.job(jid), host_config={"slots": 4, "sandbox": "optional"}, deferred_load_cap=12))
        self.assertIn('. "$D/job.env"\nSELFHEAVY=1\nexport SLOTS SELFHEAVY TIMEOUT_S SUBDIR', script)  # host/deferred.py desc() needs it
        self.assertIn('deferred.py" register "$ID"', script)
        self.assertIn(BOXED_INNER.format(net=" --net"), script)
        self.assertIn("CQ_NO_JOBBOX", script)
        plain = cq.make_launcher(dict(self.job(self.submit(where="host", host_id="ember", defer="overnight", memory_max_gb=2)),
                                      host_config={"slots": 4, "sandbox": "optional"}, deferred_load_cap=12))
        self.assertIn(PLAIN_INNER, plain)
        self.assertNotIn("jobbox", plain)

    def test_with_a_tmp_root_the_scratch_directory_is_what_the_box_opens(self):
        script = cq.make_launcher(self.make({"sandbox": "optional", "tmp_root": "/dev/shm/cq"}, sandbox=True))
        self.assertIn("/dev/shm/cq", script)
        self.assertIn('RT=$(cat "$D/ramtmp"', script)
        self.assertLess(script.index('RT=$(cat "$D/ramtmp"'), script.index("CQ_BOX_RW="))

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_scripts_parse_under_bash_n(self):
        variants = [self.make({"sandbox": "optional"}, sandbox=True, network=True), self.make({"sandbox": "required", "tmp_root": "/dev/shm/cq"})]
        for number, record in enumerate(variants):
            path = os.path.join(self.tmp, f"box{number}.sh")
            with open(path, "w", newline="\n") as f:
                f.write(cq.make_launcher(record))
            result = subprocess.run([BASH, "-n", unix(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    # -- run.sh executed in bash, with a stand-in for the helper that records its arguments ----------------------------

    def run_script(self, *, net, ramtmp=None, env_ram=None, goflags=None):
        fake = os.path.join(self.tmp, "fakebox")
        with open(fake, "w", newline="\n") as f:
            f.write('#!/bin/bash\nfor a in "$@"; do printf "%s\\n" "$a"; done > "$D/box.args"\n'
                    'printf "%s" "${GOFLAGS-unset}" > "$D/box.goflags"\n'
                    'while [ "$1" != -- ]; do shift; done; shift; exec "$@"\n')
        os.chmod(fake, 0o755)
        with mock.patch.object(cq, "JOBBOX", unix(fake)):
            script = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True, network=net))
        body = script.split("<<'RUNEOF'\n")[1].split("\nRUNEOF")[0]
        d = os.path.join(self.tmp, "run" + str(len(os.listdir(self.tmp))))
        os.makedirs(os.path.join(d, "src"))
        with open(os.path.join(d, "cmd.sh"), "w", newline="\n") as f:
            f.write("echo ran > ../ran\n")
        with open(os.path.join(d, "job.env"), "w", newline="\n") as f:
            f.write("SLOTS=0\nSELFHEAVY=0\nTIMEOUT_S=60\nSUBDIR=.\n")
        if ramtmp is not None:
            with open(os.path.join(d, "ramtmp"), "w", newline="\n") as f:
                f.write(ramtmp)
        with open(os.path.join(d, "run.sh"), "w", newline="\n") as f:
            f.write(body + "\n")
        env = dict(os.environ)
        env.pop("CQ_RAM_TMP", None)
        env.pop("CQ_BOX_RW", None)
        env.pop("GOFLAGS", None)
        if goflags is not None:
            env["GOFLAGS"] = goflags
        if env_ram is not None:
            env["CQ_RAM_TMP"] = env_ram
        result = subprocess.run([BASH, unix(os.path.join(d, "run.sh"))], capture_output=True, text=True, env=env, timeout=60)
        with open(os.path.join(d, "exit")) as f:
            self.assertEqual(f.read().strip(), "0", result.stderr)
        self.assertTrue(os.path.exists(os.path.join(d, "ran")))  # the command ran through the helper
        self.assertTrue(os.path.exists(os.path.join(d, "started")))  # written outside it
        with open(os.path.join(d, "box.args"), newline="") as f:
            return [line for line in f.read().split("\n") if line != ""], d

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_run_sh_adds_buildvcs_false_to_the_goflags_the_helper_sees(self):
        for given, expected in ((None, "-buildvcs=false"), ("-trimpath -count=1", "-trimpath -count=1 -buildvcs=false")):
            with self.subTest(given=given):
                _, d = self.run_script(net=False, goflags=given)
                with open(os.path.join(d, "box.goflags")) as f:
                    self.assertEqual(f.read(), expected)

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_run_sh_arguments_without_a_scratch_directory(self):
        args, d = self.run_script(net=False)
        self.assertEqual(args[0], "--dir")
        self.assertTrue(args[1].endswith(os.path.basename(d)), args)
        self.assertEqual(args[2:], ["--", "timeout", "-k", "30", "60", "bash", args[1] + "/cmd.sh"])

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_run_sh_arguments_with_net_and_a_scratch_directory_with_a_space(self):
        args, d = self.run_script(net=True, ramtmp="/dev/shm/cq/ab cd1234")
        self.assertEqual(args[:4], ["--net", "--dir", args[2], "--rw"])
        self.assertEqual(args[4], "/dev/shm/cq/ab cd1234")  # one argument
        self.assertEqual(args[5:8], ["--", "timeout", "-k"])

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_run_sh_opens_the_ram_service_scratch_directory(self):
        args, _ = self.run_script(net=False, env_ram="/dev/shm/cq-ram/cq-1-xyz")
        self.assertEqual(args[2:4], ["--rw", "/dev/shm/cq-ram/cq-1-xyz"])

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_launcher_without_the_helper_stops_before_making_anything(self):
        home = os.path.join(self.tmp, "home")
        os.makedirs(home)
        path = os.path.join(self.tmp, "launcher.sh")
        with mock.patch.object(cq, "JOBBOX", "/nonexistent/jobbox"):
            script = cq.make_launcher(self.make({"sandbox": "optional"}, sandbox=True))
        with open(path, "w", newline="\n") as f:
            f.write(script)
        result = subprocess.run([BASH, unix(path)], capture_output=True, text=True, env=dict(os.environ, HOME=unix(home)), timeout=60)
        self.assertEqual(result.returncode, 79)
        self.assertRegex(result.stdout, r"^CQ_NO_JOBBOX cq-")
        self.assertIn(cq.JOBBOX_MISSING, result.stderr)
        self.assertEqual(os.listdir(home), [])  # no ~/q, no job directory


class LaunchFailureTests(Base):
    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, "_CONF", {"sandbox": "optional"})
        patch.start()
        self.addCleanup(patch.stop)

    def launched(self, remoterun, **kw):
        jid = self.submit(sandbox=True, **kw)
        self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host"})["ok"])
        self.ctx.run_remoterun = remoterun
        cq.launch(self.ctx, jid)
        return jid

    def test_missing_helper_is_a_terminal_failure_with_the_reason(self):
        calls = []

        def remoterun(argv, cwd):
            calls.append(argv)
            jid = re.search(r"^ID=(\S+)", base64.b64decode(re.search(r"echo (\S+) \|", argv[-1]).group(1)).decode(), re.M).group(1)
            return 79, f"CQ_NO_JOBBOX {jid}\n", "host has no job sandbox helper (/usr/local/libexec/baton/jobbox)\n"
        jid = self.launched(remoterun)
        job = self.job(jid)
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["reason"], "host has no job sandbox helper (/usr/local/libexec/baton/jobbox)")
        self.assertEqual(len(calls), 1)  # not requeued
        self.assertEqual(job["attempts"], 1)

    def test_other_launch_failures_still_retry(self):
        jid = self.launched(lambda argv, cwd: (79, "", "something else\n"))
        self.assertEqual(self.job(jid)["status"], "queued")

    def test_a_setting_changed_since_submit_fails_the_job_with_the_refusal(self):
        jid = self.submit(sandbox=True)
        self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host"})["ok"])
        self.ctx.run_remoterun = lambda argv, cwd: self.fail("nothing may be sent to the host")
        with mock.patch.object(cq, "_CONF", {}):  # the host's setting was removed after the job was bound
            job = self.job(jid)
            job["host_config"]["sandbox"] = "off"
            cq.save_job(self.ctx, job)
            cq.launch(self.ctx, jid)
        self.assertEqual(self.job(jid)["status"], "failed")
        self.assertEqual(self.job(jid)["reason"], NO_BOX_SETTING)

    def test_a_host_made_required_after_dispatch_boxes_the_launch_or_refuses_an_opt_out(self):
        sent = []
        jid = self.submit()  # asked for nothing, bound while the host was off
        with mock.patch.object(cq, "_CONF", {}):
            self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host"})["ok"])
        self.assertEqual(self.job(jid)["host_config"]["sandbox"], "off")
        self.ctx.run_remoterun = lambda argv, cwd: (sent.append(base64.b64decode(re.search(r"echo (\S+) \|", argv[-1]).group(1)).decode())
                                                    or (0, f"CQ_LAUNCHED {jid}\n", ""))
        with mock.patch.object(cq, "_CONF", {"sandbox": "required"}):
            cq.launch(self.ctx, jid)
        self.assertEqual(self.job(jid)["status"], "running")
        self.assertIn("exec /usr/local/libexec/baton/jobbox", sent[0])
        optout = self.submit(sandbox=False)
        with mock.patch.object(cq, "_CONF", {}):
            self.assertTrue(cq.dispatch(self.ctx, {"job": optout, "where": "host"})["ok"])
        self.ctx.run_remoterun = lambda argv, cwd: self.fail("nothing may be sent to the host")
        with mock.patch.object(cq, "_CONF", {"sandbox": "required"}):
            cq.launch(self.ctx, optout)
        self.assertEqual((self.job(optout)["status"], self.job(optout)["reason"]), ("failed", REQUIRED))

    def test_ram_job_takes_the_ram_settlement_path(self):
        with mock.patch.object(cq, "RAM_JOBS", True):
            jid = self.submit(sandbox=True, temp_storage="ram", memory_max_gb=1)
            self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host"})["ok"])
            self.ctx.run_remoterun = lambda argv, cwd: (79, f"CQ_NO_JOBBOX {jid}\n", "")
            cq.launch(self.ctx, jid)
        job = self.job(jid)
        self.assertEqual(job["status"], "launching")  # reap_ram stops and settles it, as for a failed cq-job check
        self.assertEqual(job["launch_error"], "host has no job sandbox helper (/usr/local/libexec/baton/jobbox)")

    def test_deferred_job_fails_instead_of_waiting_for_a_registration(self):
        jid = self.submit(sandbox=True, where="host", host_id="ember", defer="overnight", memory_max_gb=2)
        self.assertTrue(cq.dispatch(self.ctx, {"job": jid, "where": "host"})["ok"])
        self.ctx.run_remoterun = lambda argv, cwd: (79, f"CQ_NO_JOBBOX {jid}\n", "")
        cq.launch(self.ctx, jid)
        self.assertEqual(self.job(jid)["status"], "failed")
        self.assertEqual(self.job(jid)["reason"], cq.JOBBOX_MISSING)


class FleetSandboxTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "work"
        (self.root / ".baton").mkdir(parents=True)
        (self.root / ".baton" / "owner.json").write_text(json.dumps({"session_id": "s1", "vendor": "codex"}))
        self.state = Path(self.tmp.name) / "state"
        self.state.mkdir()
        cq.write_json(str(self.state / "hosts.json"), {"version": 1, "hosts": {
            "hearth": {"state": "hosts/hearth", "enabled": True}, "forge": {"state": "hosts/forge", "enabled": True}}})
        cq.write_json(str(self.state / "host.conf.json"), {"address": "ember", "host_id": "ember", "sandbox": "optional"})
        cq.write_json(str(self.state / "hosts/hearth/host.conf.json"), {"address": "hearth", "host_id": "hearth"})
        cq.write_json(str(self.state / "hosts/forge/host.conf.json"), {"address": "forge", "host_id": "forge", "sandbox": "required"})
        self.clock = [1700000000.0]
        self.ctx = cq.Ctx(state=str(self.state), now=lambda: self.clock[0], sync=True)
        self.ctx.host_id = None
        fq.bind(cq)
        old_file, old_load = cq.job_file, cq.load_job
        for name, target in (("job_file", lambda ctx, jid, deferred=False: ctx.p("fleet", "jobs", jid + ".json")
                                if jid.startswith(("cq-fleet-", "cq-v2--fleet-")) else old_file(ctx, jid, deferred)),
                             ("load_job", lambda ctx, jid: cq.read_json(ctx.p("fleet", "jobs", jid + ".json"))
                                if jid.startswith(("cq-fleet-", "cq-v2--fleet-")) else old_load(ctx, jid))):
            patch = mock.patch.object(cq, name, side_effect=target)
            patch.start()
            self.addCleanup(patch.stop)

        def fake_git(top, *args, **kw):
            if args == ("rev-parse", "--show-toplevel"):
                return str(self.root)
            if args == ("rev-parse", "--git-common-dir"):
                return ".git"
            if args and args[0] == "rev-parse":
                return "a" * 40
            raise AssertionError(args)
        for name, kw in (("git", dict(side_effect=fake_git)), ("snapshot", dict(return_value=("a" * 40, False)))):
            patch = mock.patch.object(cq, name, **kw)
            patch.start()
            self.addCleanup(patch.stop)

    def submit(self, **changes):
        req = dict(where="fleet", root=str(self.root), command="echo test", session="s1", slots=1)
        req.update(changes)
        return fq.execute(self.ctx, "submit", req)

    def job(self, jid):
        return cq.load_job(self.ctx, jid)

    def remote(self, host, sandbox=None):
        policy = dict(pool_slots=8, load_cap=20, ram_floor_gb=4)
        data = dict(policy=policy, load=0, available_bytes=40 * fq.GIB, total_bytes=64 * fq.GIB, active_fleet_slots=0,
                    reserved_memory_bytes=0, legacy_busy=False, ordinary_waiting=False, active_fleet=[], terminal_fleet=[],
                    capabilities=["linux", "python"])
        binding = dict(address=host, machine_id="a" * 32, slots=8, slot_vcpu=1.5, ram_floor_gb=4, load_cap=20)
        if sandbox:
            binding["sandbox"] = sandbox
        return dict(host=host, state=str(self.state), binding=binding, data=data, at=self.clock[0])

    def test_fields_are_carried_unchanged(self):
        job = self.job(self.submit(candidates=["ember"], sandbox=True, network=True)["job"])
        self.assertIs(job["sandbox"], True)
        self.assertIs(job["network"], True)
        job = self.job(self.submit(candidates=["ember"])["job"])
        self.assertIsNone(job["sandbox"])
        self.assertIs(job["network"], False)
        self.assertEqual(job["candidates"], ["ember"])

    def test_type_errors(self):
        for kw, message in (({"sandbox": "yes"}, "sandbox must be true or false"), ({"network": 1}, "network must be true or false")):
            with self.assertRaisesRegex(cq.CqError, message):
                self.submit(candidates=["ember"], **kw)

    def test_linux_only(self):
        for kw in (dict(platform="windows", candidates=["laptop"]), dict(platform="portable"), dict(platform="portable", candidates=["laptop"]),
                   dict(candidates=["laptop"])):
            with self.subTest(kw=kw), self.assertRaisesRegex(cq.CqError, "the job sandbox exists only on Linux hosts"):
                self.submit(sandbox=True, **kw)
        # without the request the same submissions are unchanged
        self.assertTrue(self.submit(platform="windows", candidates=["laptop"])["ok"])

    def test_candidates_are_the_hosts_that_take_the_request(self):
        self.assertEqual(self.job(self.submit(sandbox=True)["job"])["candidates"], ["ember", "forge"])  # hearth: sandbox off
        self.assertEqual(self.job(self.submit(sandbox=False)["job"])["candidates"], ["ember", "hearth"])  # forge: required
        self.assertEqual(self.job(self.submit()["job"])["candidates"], ["ember", "forge", "hearth"])  # no request: nothing filtered
        with self.assertRaisesRegex(cq.CqError, re.escape(NO_BOX_SETTING)):
            self.submit(sandbox=True, candidates=["hearth"])
        with self.assertRaisesRegex(cq.CqError, re.escape(REQUIRED)):
            self.submit(sandbox=False, candidates=["forge"])

    def test_placement_skips_a_host_whose_probed_setting_refuses(self):
        jid = self.submit(sandbox=True, candidates=["ember", "forge"])["job"]
        probes = {"ember": self.remote("ember", "off"), "forge": self.remote("forge", "required")}
        chosen = fq._reserve(self.ctx, probes)
        self.assertEqual([j["host_id"] for j in chosen], ["forge"])
        self.assertEqual(self.job(jid)["host_config"]["sandbox"], "required")
        jid = self.submit(sandbox=True, candidates=["ember"])["job"]
        self.assertEqual(fq._reserve(self.ctx, {"ember": self.remote("ember", "off")}), [])
        self.assertEqual(self.job(jid)["placement_reasons"], {"ember": NO_BOX_SETTING})

    def test_staging_boxes_the_job_and_a_missing_helper_fails_it(self):
        jid = self.submit(sandbox=True, network=True, candidates=["ember"])["job"]
        job = fq._reserve(self.ctx, {"ember": self.remote("ember", "optional")})[0]
        sent = []

        def run(argv, cwd):
            sent.append(base64.b64decode(argv[-1].split(" ")[1]).decode())
            return 79, f"CQ_NO_JOBBOX {jid}\n", "host has no job sandbox helper\n"
        self.ctx.run_remoterun = run
        fq._stage_remote(self.ctx, job)
        self.assertIn("jobbox --net --dir", sent[0])
        after = self.job(jid)
        self.assertEqual((after["status"], after["reason"]), ("failed", cq.JOBBOX_MISSING))

    def test_staging_with_a_setting_that_no_longer_takes_the_request_fails_it(self):
        jid = self.submit(sandbox=True, candidates=["ember"])["job"]
        job = fq._reserve(self.ctx, {"ember": self.remote("ember", "optional")})[0]
        cq.write_json(str(self.state / "host.conf.json"), {"address": "ember", "host_id": "ember"})  # the setting was removed
        stored = self.job(jid)
        stored["host_config"]["sandbox"] = "off"
        cq.save_job(self.ctx, stored)
        self.ctx.run_remoterun = lambda argv, cwd: self.fail("nothing may be sent to the host")
        fq._stage_remote(self.ctx, self.job(jid))
        after = self.job(jid)
        self.assertEqual((after["status"], after["reason"]), ("failed", NO_BOX_SETTING))
        self.assertNotIn("stage_attempted", after)

    def test_staging_re_reads_the_host_setting_the_probe_binding_may_be_older_than_it(self):
        """The binding is copied from a probe when the host is reserved; staging can come minutes later."""
        sent = []

        def run(argv, cwd):
            sent.append(base64.b64decode(argv[-1].split(" ")[1]).decode())
            return 0, "", ""
        for conf, request, outcome in (({"sandbox": "required"}, {}, "boxed"), ({"sandbox": "required"}, {"sandbox": False}, REQUIRED),
                                       ({"sandbox": "optional"}, {}, "unboxed"), ({"sandbox": "typo"}, {}, INVALID)):
            with self.subTest(conf=conf, request=request):
                cq.write_json(str(self.state / "host.conf.json"), {"address": "ember", "host_id": "ember"})
                jid = self.submit(candidates=["ember"], **request)["job"]
                job = fq._reserve(self.ctx, {"ember": self.remote("ember", "off")})[0]
                self.assertEqual(self.job(jid)["host_config"].get("sandbox", "off"), "off")  # bound while the host was off
                cq.write_json(str(self.state / "host.conf.json"), {"address": "ember", "host_id": "ember", **conf})
                sent.clear()
                self.ctx.run_remoterun = run
                fq._stage_remote(self.ctx, self.job(jid))
                after = self.job(jid)
                if outcome in ("boxed", "unboxed"):
                    self.assertEqual(len(sent), 1)
                    self.assertEqual("exec /usr/local/libexec/baton/jobbox" in sent[0], outcome == "boxed")
                else:
                    self.assertEqual(sent, [])
                    self.assertEqual((after["status"], after["reason"]), ("failed", outcome))
                cq.save_job(self.ctx, dict(self.job(jid), status="failed"))  # the next case reserves the host afresh

    def test_an_invalid_host_setting_takes_no_fleet_job(self):
        cq.write_json(str(self.state / "hosts/hearth/host.conf.json"), {"address": "hearth", "host_id": "hearth", "sandbox": "Required"})
        self.assertEqual(self.job(self.submit()["job"])["candidates"], ["ember", "forge"])  # hearth is left out, even unasked
        with self.assertRaisesRegex(cq.CqError, INVALID):
            self.submit(candidates=["hearth"])
        jid = self.submit(candidates=["ember", "forge"])["job"]
        self.assertEqual(fq._reserve(self.ctx, {"ember": self.remote("ember", "invalid")}), [])
        self.assertEqual(self.job(jid)["status"], "queued")
        self.assertEqual(self.job(jid)["placement_reasons"]["ember"], INVALID)


class GenerationRoutingTests(Base):
    """sandbox and network alone never move a submission between the queue generations."""

    def route(self, **kw):
        calls = []

        def leaf(ctx, command, request):
            calls.append((ctx.generation, dict(request)))
            return {"ok": True}
        queue_generations.execute(cq, self.ctx, "submit", {"root": self.repo, "session": "sess-A", "command": "x", **kw}, leaf)
        generation, routed = calls[0]
        return generation, {k: v for k, v in routed.items() if k not in ("sandbox", "network")}

    def test_routing_is_the_same_with_and_without_the_fields(self):
        for kw in ({}, {"where": "host"}, {"where": "host", "host_id": "hestia"}, {"where": "any"}, {"where": "laptop"},
                   {"where": "fleet"}, {"where": "fleet", "candidates": ["hestia"], "platform": "windows"},
                   {"where": "host", "host_id": "hestia", "gets": ["a.png"], "slots": 2, "memory_max_gb": 3},
                   {"where": "host", "artifact_max_mb": 5}):
            for extra in ({"sandbox": True}, {"sandbox": False, "network": True}, {"network": True}):
                with self.subTest(kw=kw, extra=extra):
                    self.assertEqual(self.route(**kw, **extra), self.route(**kw))

    def test_fields_reach_the_leaf_of_either_generation(self):
        calls = []
        queue_generations.execute(cq, self.ctx, "submit", {"root": self.repo, "session": "sess-A", "command": "x", "where": "fleet",
                                                           "sandbox": True, "network": True},
                                  lambda ctx, command, request: calls.append((ctx.generation, request)) or {"ok": True})
        queue_generations.execute(cq, self.ctx, "submit", {"root": self.repo, "session": "sess-A", "command": "x", "sandbox": True,
                                                           "slots": 0},   # light work stays an ordinary job
                                  lambda ctx, command, request: calls.append((ctx.generation, request)) or {"ok": True})
        # a heavy where: host job is a fleet job too, and its fields go with it
        queue_generations.execute(cq, self.ctx, "submit", {"root": self.repo, "session": "sess-A", "command": "x", "sandbox": True,
                                                           "network": True},
                                  lambda ctx, command, request: calls.append((ctx.generation, request)) or {"ok": True})
        self.assertEqual([c[0] for c in calls], [2, 1, 2])
        self.assertTrue(all(c[1]["sandbox"] is True for c in calls))
        self.assertEqual((calls[2][1]["where"], calls[2][1]["candidates"], calls[2][1]["network"]), ("fleet", ["ember"], True))


if __name__ == "__main__":
    unittest.main()
