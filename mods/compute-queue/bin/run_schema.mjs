export default {
  "type": "object",
  "properties": {
    "command": {
      "type": "string",
      "description": "Allowlisted job launcher. Put authorized environment/setup or complex arguments in a saved script (for example python3 .baton/remote/verify.py); inline environment names and wildcard arguments can be refused. Existing permission limits still apply."
    },
    "subdir": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "where": {
      "type": "string",
      "enum": [
        "host",
        "laptop",
        "any",
        "fleet"
      ],
      "description": "fleet selects a compatible configured device using independent CPU and RAM requirements. host (the default) with slots >= 1 and no host_id, or host_id ember, runs as a contained fleet job on ember: CPU held to the slots, memory capped (memory_max_gb, else the larger of 8 GB and 4 GB per slot). Another host's id (or an alias) does the same only when that host's host.conf.json sets host_jobs_to_fleet true. Otherwise, and for slots 0, split, self_heavy and RAM jobs, the ordinary queue keeps the job."
    },
    "platform": {
      "type": "string",
      "enum": [
        "linux",
        "windows",
        "portable"
      ],
      "description": "Fleet compatibility declaration; default linux. portable explicitly allows the same command on Windows and Linux."
    },
    "candidates": {
      "type": "array",
      "minItems": 1,
      "maxItems": 64,
      "items": {
        "type": "string",
        "pattern": "^[a-z][a-z0-9_]{0,31}$"
      },
      "description": "Optional configured fleet host IDs or aliases, including laptop. Selection is pinned before staging."
    },
    "requires": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "python",
          "bash",
          "go",
          "node",
          "ffmpeg",
          "blender",
          "adb"
        ]
      },
      "description": "Required executable tools; unavailable or unverified capabilities exclude a device."
    },
    "memory_gb": {
      "type": "number",
      "minimum": 0.25,
      "maximum": 50,
      "description": "Expected fleet job memory in GiB, independent of CPU slots. Admission reserves the full enforced cap conservatively."
    },
    "resource_key": {
      "type": "string",
      "description": "Optional recipe label for conservative resource-history suggestions; source, command, platform, host and slots still constrain matching."
    },
    "host_id": {
      "type": "string",
      "description": "Explicit configured remote host ID or alias with where=host. Omit for the primary queue."
    },
    "slots": {
      "type": "integer",
      "minimum": 0,
      "maximum": 32,
      "description": "CPU slots the command needs (default 1; fleet requires at least 1). Fleet RAM uses memory_gb independently; legacy queues retain their configured per-slot allowance. Declare the CPU the job actually uses."
    },
    "self_heavy": {
      "type": "boolean"
    },
    "temp_storage": {
      "type": "string",
      "enum": [
        "disk",
        "ram"
      ]
    },
    "memory_max_gb": {
      "type": "number",
      "minimum": 0.25,
      "maximum": 50,
      "description": "Enforced job memory cap in GiB: Linux cgroup memory, Windows aggregate committed memory. Required for pinned RAM/deferred jobs; fleet can derive a conservative cap from its estimate."
    },
    "sandbox": {
      "type": "boolean",
      "description": "Run the job in the host's sandbox: it sees its own source copy, the host's configured toolchains and caches, and nothing else of the queue user's home. No network unless network is true. Needed on hosts set to require it; refused on hosts that have none. Linux hosts only."
    },
    "network": {
      "type": "boolean",
      "description": "For a sandboxed job: use the host's network (module downloads, external services). Default: loopback only."
    },
    "defer": {
      "type": "string",
      "enum": [
        "spare",
        "overnight"
      ],
      "description": "Host-managed spare capacity or the configured overnight window (default Europe/London 00:00-08:00). Use a pinned host or fleet remote candidates. Confirm deferred_registered before laptop sleep."
    },
    "est_minutes": {
      "type": "integer",
      "minimum": 1,
      "maximum": 100000
    },
    "priority": {
      "type": "string",
      "enum": [
        "verify",
        "fixcheck",
        "lane",
        "background"
      ]
    },
    "timeout_minutes": {
      "type": "integer",
      "minimum": 1,
      "maximum": 480
    },
    "label": {
      "type": "string"
    },
    "gets": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "Distinct portable source-relative files, at most 1024 UTF-8 bytes each and 8192 total. Requires host or fleet execution; local artifacts need fleet platform windows. No split artifacts. Size/SHA-256 verified with pinned limits; missing captures retain source for refresh. Inspect actual files before reviewing results.",
      "maxItems": 64
    },
    "split": {
      "type": "object",
      "properties": {
        "max_parts": {
          "type": "integer",
          "minimum": 2,
          "maximum": 8
        }
      },
      "required": [
        "max_parts"
      ],
      "additionalProperties": false
    },
    "artifact_max_mb": {
      "type": "integer",
      "minimum": 1,
      "maximum": 1024,
      "description": "Per-file artifact capture limit in MiB, fixed at submission; default 256. Must not exceed artifact_total_mb."
    },
    "artifact_total_mb": {
      "type": "integer",
      "minimum": 1,
      "maximum": 4096,
      "description": "Aggregate artifact capture budget in MiB across files and retries; default 1024. Does not increase job RAM."
    }
  },
  "required": [
    "command"
  ],
  "additionalProperties": false
};
