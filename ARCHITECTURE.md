# MeshKore daemon — architecture map

> Read this first. It exists so an AI assistant (or a human) can walk into
> `daemon/` and know **where to look for X** without reading every file.
> The daemon is authored as ~80 small, single-responsibility modules and
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
| `yamlparse.py` | `parse_simple_yaml` + `parse_frontmatter` + `split_frontmatter` + `_FM_RE` (`registries._split_frontmatter` re-exports the splitter) |
| `timeline.py` | timeline JSONL iter/read/append |
| `utils.py` | daemon logger `_log`, debug-stream singleton, TLS bundle discovery; **re-exports** timeutil/yamlparse/timeline so `from utils import …` still works |
| `nethttp.py` | the ONE outbound HTTP fetch (`fetch_bytes`/`fetch_text`/`fetch_head_bytes`) — scheme allow-list + read ceiling + one User-Agent convention. Every CDN/TLS/standard/API call goes through it |
| `sweeper.py` | `ProjectSweeper` — the per-project background loop (bind project → `tick()` → unbind) shared by `ChatSessionReaper` and `QuotaProber` |
| `wsframe.py` | the ONE RFC-6455 frame codec (`encode`/`encode_text`/`close_frame`/`read_frame`) + the inbound payload ceiling. Shared by `hub.WSClient` (unmasked server writes), `routes._ws_read_frame` (masked server reads) and `verify._WS` (masking client) — three different halves of the protocol that each used to carry their own copy of the arithmetic |
| `debuglog.py` | `DebugLog` ring stream (`/debug/tail`) |
| `agent_prompts/` | the declarative `AGENT_PROMPTS` registry (split into per-role fragments + `_roadmap_architect` SOP) |
| `agent_types.py` | agent-type resolution (`_agent_manifest`, `_agent_type_normalised`, `_agent_type_from_conv_slug`) |
| `runnerutil.py` | `_session_id_for_conv`, `_find_claude` |

### Layer 1 — components (one class/concern each, depend only on leaves)
| module | owns |
|---|---|
| `cluster.py` | `Cluster` (cluster.yaml + crons validation), `normalize_status` (what the board renders) + `is_resolved_status` (whether work remains — NOT the same question; see DAH7), `_patch_frontmatter` |
| `hub.py` | `Hub` / `WSClient` — the WebSocket broadcast hub. `WSClient` owns the per-connection send `RLock` (py-1.31.4: two threads in `SSL_write` on one socket is native heap corruption) and the send timeout; the frame encoding is `wsframe`'s |
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
| `WallsMixin` (`walls`) | `/initiative/walls` + `/initiative/reorder` (roadmap wall ordering) |
| `TeamMixin` (`teamsvc`) | `/team` CRUD + `_member_dispatch_prep` + `/team/draft` (LLM-backed) |
| `TeamExtMixin` (`teamext`) | Team External Gateway: `/team/<id>/ask`, `/team/requests/<rid>`, member-token lifecycle, the A2A card, the per-request watcher |
| `ClientsMixin` (`clidriverssvc`) | `GET /clients` — CLI-client catalog + local usability probes |
| `ProvidersMixin` (`providersvc`) | `/config/providers` + the machine-global key store + `resolve_provider` |
| `VerifyMixin` (`verifysvc`) | `POST /verify` — local CDP run or remote A2A verify agent |
| `SnapshotsMixin` (`snapshots`) | Standard §20 file snapshots: `POST/GET/DELETE /snapshots*` |
| `ProjectsMixin` (`projectsapi`) | `/projects` — the GLOBAL project registry (adopt / create / unregister) |
| `RemoteControlMixin` (`remotectl`) | `/remote/token` — the machine-level remote-control credential (CPL-2) |
| `StateManager` (`state.py`) | the FS-poll loop (a held object, not a mixin) |
| `ProjectContext` (`projectctx.py`) | all PER-PROJECT state (paths, cluster, state_manager, runs, chat_sessions, queue, uploads, quota, registries, cron…) — a held object, not a mixin. DC-1 of `daemon-centralized`: the seam for one-daemon-many-projects. The Daemon holds one today (+ aliases `self.<attr> = ctx.<attr>`); DC-2 turns it into a registry keyed by project_id. GLOBAL services (hub, identity, token, port, http server, VersionWatcher) stay on the Daemon. |

### Layer 3 — composition root
| module | owns |
|---|---|
| `routes.py` (+ `routes_get`/`routes_post`) | the HTTP `make_handler` closure; `_do_GET`/`_do_POST` delegate to the route-table functions. NOTE: only GET and POST are extracted — the PUT/PATCH/DELETE tables are still inline in `routes.py`. **Auth is fail-closed on all three wire surfaces** (it was not, until `daemon-audit-hardening`): POST has always had ONE global gate; GET got its own in py-1.34.0 (DAH2a), driven by the declarative `PUBLIC_GET_EXACT`/`PUBLIC_GET_PREFIXES` tables, so a new GET is private unless someone deliberately edits that table; and the **WebSocket upgrade** got `_ws_authorized` in py-1.35.0 (DAH4) — it is resolved before the HTTP gate and had no gate of its own, so `/events` streamed every hub broadcast, machine-wide, to anyone who could open a socket. The WS gate is separate rather than folded into `_need_auth` because a browser cannot set a header on a `new WebSocket(...)`: it reads `?token=`, and checks `Origin` too. `tests/test_auth_matrix.py` + `tests/test_ws_auth.py` are the guards — they probe every route (and both upgrade aliases) anonymously against a live daemon and diff against an explicit record, so a newly-public surface goes red instead of unnoticed |
| `daemon.py` | imports every mixin, `class Daemon(…24 mixins…)`, `__init__` wiring, `main`/`_parse_args` |

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
- `tests/test_snapshots.py` — the Standard §20 contract (create/list/manifest/
  raw-read/delete, traversal refusals, daily-log integration, auth).
- `tests/test_teamext_stream_match.py` — an external ask resolves to ITS OWN
  turn's final, never a neighbouring turn's.
- `tests/test_auth_matrix.py` — every route's anonymous reachability, probed
  live and pinned with the reason each public route is public.
- `tests/test_cors_allowlist.py` — the origin allowlist names OUR surfaces, not
  a hosting domain (adversarial near-misses included).
- `tests/test_ws_auth.py` — the WebSocket upgrade refuses an anonymous caller,
  a hostile `Origin` (even holding a valid token) and an empty `?token=`, on
  both `/events` and `/ws`; and still upgrades for the three legitimate
  channels (cockpit `?token=`, CLI Bearer, cockpit Origin).
- `tests/test_wsframe.py` — the frame codec: every length boundary masked and
  unmasked, mask symmetry, the payload ceiling refused before the body is
  read, control opcodes, fragmentation, truncation.
- `tests/test_initiative_reconcile.py` — a task closed as `cancelled` must not
  hold its initiative open, and an initiative whose tasks were ALL abandoned
  must not archive as `done`.
- Run `pytest daemon/tests/ -q`. Rebuild the bundle (`python daemon/bundle.py`)
  before parity runs. Coverage: `pytest --cov` (data confined to
  `tests/.coverage_cache/`).

## Why mixins (Phase E2 decision)

`Daemon` inherits ~24 mixins rather than composing ~24 service objects. This was
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
