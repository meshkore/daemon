# MeshKore daemon — architecture map

> Read this first. It exists so an AI assistant (or a human) can walk into
> `daemon/` and know **where to look for X** without reading every file.
> The daemon is authored as ~60 small, single-responsibility modules and
> **bundled** into one self-contained `dist/daemon.py` for distribution
> (stdlib-only, no pip). Source you edit lives in `daemon/*.py`; never edit
> `dist/daemon.py` — run `python daemon/bundle.py` instead.

## The one rule that shapes everything: the bundle

`bundle.py` concatenates every module in `MODULES` order (leaves first) and
appends `daemon.py` last, stripping all sibling `import` lines so the result
is **one flat module namespace**. Consequences you must respect:

- **Dependency order = `MODULES` order.** A module may only rely (at
  *import/class-definition* time) on names defined earlier in the list.
  Runtime calls (inside methods) resolve against the flat namespace, so
  method bodies can reference anything.
- **No import cycles at module load.** If A imports B and B imports A, the
  source tree breaks (the bundle wouldn't, but we keep source runnable).
  The fix used throughout: push the shared symbol down to a leaf both can
  import (e.g. `timeutil._iso_now`, `agent_types`, `runnerutil`).
- **`if TYPE_CHECKING:` imports are stripped wholesale** by the bundler —
  safe for annotations, never for runtime.
- **Re-exports keep call sites stable.** When a symbol moves, its old home
  often re-exports it (`# noqa: F401`) so the ~N importers don't churn.

## Layers (top of bundle → bottom)

### Layer 0 — leaves (pure helpers, no daemon knowledge)
| module | owns |
|---|---|
| `constants.py` | `DAEMON_VERSION`, port range, FS-poll interval, registry paths |
| `paths.py` | `Paths` (every `.meshkore/` path) + TLS filename constants |
| `timeutil.py` | `_iso_now` / `_iso_at` (UTC ISO-8601) |
| `yamlparse.py` | `parse_simple_yaml` + `parse_frontmatter` + `_FM_RE` |
| `timeline.py` | timeline JSONL iter/read/append |
| `utils.py` | daemon logger `_log`, debug-stream singleton, TLS bundle discovery; **re-exports** timeutil/yamlparse/timeline so `from utils import …` still works |
| `debuglog.py` | `DebugLog` ring stream (`/debug/tail`) |
| `agent_prompts/` | the declarative `AGENT_PROMPTS` registry (split into per-role fragments + `_roadmap_architect` SOP) |
| `agent_types.py` | agent-type resolution (`_agent_manifest`, `_agent_type_normalised`, `_agent_type_from_conv_slug`) |
| `runnerutil.py` | `_session_id_for_conv`, `_find_claude` |

### Layer 1 — components (one class/concern each, depend only on leaves)
| module | owns |
|---|---|
| `cluster.py` | `Cluster` (cluster.yaml + crons validation), `normalize_status`, `_patch_frontmatter` |
| `hub.py` | `Hub` / `WSClient` — the WebSocket broadcast hub |
| `registries.py` / `protocols.py` | `LinksRegistry` / `ProtocolsRegistry` |
| `integrity.py` / `integritycheck.py` | `ProjectState` / `StateIntegrityChecker` |
| `statebuild.py` | `build_state` — FS → state.json projection |
| `render.py` | `AgentInstructionsRenderer` (§17 per-CLI render) |
| `runs.py` / `runrotator.py` | `RunStore` / `TimelineRotator` |
| `storage.py` / `uploads.py` / `chatqueue.py` | `ChatArchive`+`StorageReport` / `UploadStore` / `ChatQueueManager` |
| `chat.py` / `chatreaper.py` | `ChatSessions` / `ChatSessionReaper` |
| `http_server.py` | `PoolHTTPServer` (bounded thread pool, TLS, keep-alive) |
| `bootstrap.py` / `bootupdate.py` | pre-Daemon identity/token/port + boot self-update |
| `selfupdate.py` / `quota.py` / `quotaprober.py` / `cron.py` / `cronsched.py` | `VersionWatcher` / `QuotaState` / `QuotaProber` / `CronRunner` / `CronScheduler` |
| `prompts.py` | `BriefingPipeline` — composes the prompt for one agent turn (+ `_conversation_history`) |
| `runner.py` (+ `runneranchor`/`runnerloop`/`runnerspawn`) | `ChatRunner` — spawns + reads one `claude -p` subprocess. Big methods live in mixins it inherits. |

### Layer 2 — Daemon facets (mixins inherited by `Daemon`)
These share `self` on the combined `Daemon` instance. Each is one slice of
behaviour:
| mixin (module) | endpoint/behaviour surface |
|---|---|
| `QueryMixin` (`readapi`) | `/health`, `/info`, `/agents`, `_features` |
| `FsReadMixin` (`fsread`) | `context_tree`, `log_listing`, `initiative_activity` |
| `ChatReadMixin` (`chatread`) | `chat_convs`, `chat_snapshot`, conv message reads |
| `CredMixin` (`credapi`) | credentials CRUD |
| `ChatMixin` (`chatsvc`) | `chat_dispatch` / `chat_cancel` / archive |
| `ChatSpawnMixin` (`chatspawn`) | `_spawn_chat_turn` + queue flush |
| `ConvMetaMixin` (`convmeta`) | the `conv_meta.json` sidecar accessors |
| `CrudMixin` (`crud`) | runs / tasks / agents / message CRUD |
| `CoordinationMixin` (`coordination`) | `_dispatch_mutex_check` (dispatch invariants) |
| `WakeMixin` (`coordwake`) | architect-wake + dependency gating |
| `PauseMixin` (`pausemgr`) | agent-type pause + roadmap-pass detection |
| `AnchorMixin`/`AnchorProgressMixin` (`anchor`/`anchorprogress`) | the v23 anchor protocol side-effects |
| `LifecycleMixin` (`lifecycle`) | `serve_forever` / `request_shutdown` / `cleanup` |
| `SelfUpdateMixin` (`selfupdatesvc`) | `self_update` (download + validate + swap) |
| `StateManager` (`state.py`) | the FS-poll loop (a held object, not a mixin) |

### Layer 3 — composition root
| module | owns |
|---|---|
| `routes.py` (+ `routes_get`/`routes_post`) | the HTTP `make_handler` closure; `_do_GET`/`_do_POST` delegate to the route-table functions |
| `daemon.py` | imports every mixin, `class Daemon(…15 mixins…)`, `__init__` wiring, `main`/`_parse_args` |

## Where do I look for X?
- **A new HTTP route** → add the dispatch line in `routes_get.py`/`routes_post.py`,
  the handler method on the right mixin, and an entry in
  `tests/test_route_coverage.py` (the warranty fails otherwise).
- **The prompt an agent sees** → `prompts.py` (`BriefingPipeline`) + the role
  text in `agent_prompts/`.
- **What `/state` returns** → `statebuild.build_state`.
- **Chat turn lifecycle** → dispatch (`chatsvc`) → spawn (`chatspawn`) →
  subprocess (`runner*`) → finalise/anchor (`anchor*`).
- **Version / self-update** → `constants.DAEMON_VERSION`, `bootupdate`,
  `selfupdate` (watcher), `selfupdatesvc` (the endpoint).

## Testing contract
- `tests/test_parity.py` — bundle must answer byte-identically to source.
- `tests/test_refactor_characterization.py` — briefing SHAs, argv, anchor strip.
- `tests/test_route_coverage.py` — every dispatched route is enumerated +
  live-exercised (the endpoint warranty); drift fails the build.
- `tests/test_frontend_contract.py` — every response shape the cockpit
  (`architect/src/lib/daemon-client.ts`) consumes is asserted present + typed —
  the gate that proves the refactor never breaks the current frontend.
- `tests/test_chat_dispatch_integration.py` — the full dispatch→spawn→stream→
  finalise chain via a fake `claude` on the daemon PATH.
- Run `pytest daemon/tests/ -q`. Rebuild the bundle (`python daemon/bundle.py`)
  before parity runs. Coverage: `pytest --cov` (data confined to
  `tests/.coverage_cache/`).

## Why mixins (Phase E2 decision)

`Daemon` inherits ~15 mixins rather than composing ~15 service objects. This was
deliberate and is the **kept** end-state:

- **Each mixin is already one cohesive concern** — the separation-of-
  responsibilities goal is met at the module level. The mixin boundary == the
  responsibility boundary.
- **They genuinely share broad daemon state.** A dispatch check reads
  `self.chat_sessions` + `self._conv_meta_load()` + `self.quota`; a chat spawn
  touches `self.hub` + `self.cluster` + `self.upload_store` + `self.runs`.
  Threading all of that through explicit service constructors would add
  ceremony without reducing coupling — the coupling is intrinsic to "one daemon
  orchestrating one cluster".
- **The known downside — implicit method resolution** (you can't tell from
  `self._dispatch_mutex_check(...)` which file owns it) — is paid down by THIS
  map: the Layer-2 table says exactly which mixin owns which surface. An LLM
  reads the table, not the MRO.

**Rule for adding a facet:** new behaviour = a new mixin module with (1) a
top docstring naming the `self.*` it depends on, (2) one responsibility, (3) an
entry in the Layer-2 table above, (4) added to `class Daemon(...)` + `MODULES`.
If a facet ever needs to be unit-tested in isolation or reused outside Daemon,
*then* promote it to a composed service with an explicit dependency dataclass —
not before. (`StateManager` is already that shape: a held object, not a mixin,
because the FS-poll loop runs independently.)
