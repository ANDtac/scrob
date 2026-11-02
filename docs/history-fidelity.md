# Household history fidelity extension

This fork starts at Scrob v2.37.0, upstream commit
`687156b8e5f79578650479124a645736049231ee`. It retains the existing application
routes and adds authenticated `/imports/v1` APIs and the private `/imports` page.

## Data and import contract

`backend/fidelity_schemas.py` defines schema version1. A manifest is scoped to
the authenticated owner, one provider account, a source-file SHA256, parser
version and idempotency key. At most2,000 records and20MiB are accepted per batch.
Split larger exports into stable chunks. Resolve media identities before preview;
episode provider IDs identify episodes, with separately resolved parent IDs and
canonical numbering. Title-only matching is deliberately unsupported.

Occurrence precision is `instant`, `date`, `local_datetime`, or `unknown`.
Existing dates retain `legacy` precision. Date-only and unknown occurrences have
NULL `watched_at`; their calendar date or unresolved local timestamp has its own
field. No timezone, viewing time, or DST fold is inferred. Source record creation
time belongs in evidence, never in the viewing occurrence without supporting proof.

Source identity is `(owner, source, account, kind, external_id)`. Each distinct
payload creates an immutable version; a repeated payload is a no-op. Distinct
watch event IDs preserve unknown-dated rewatches. Status assertions attach to an
existing completed item and cannot establish another rewatch. Explicit reviewed
`match_event_id` links cross-source evidence to an existing owned viewing.
Changed source watch dates are retained for review without silently rewriting the
canonical date. Identical review text attached to different source IDs remains
separate, and a review can link to its exact viewing.

Preview stores a private batch and hashes the current owner state. Commit locks
the owner, checks both returned hashes and rechecks state. It briefly locks the
canonical tables to exclude older writers that do not use owner locking, then
commits the whole bounded batch or rolls back. There is no HTTP, scheduler,
playback-progress deletion, outbound fan-out or connection mutation in this path.
Hold/drain relevant automatic pulls around a controlled production migration and
restore their settings afterward. A changed state requires another preview.

Set Zach's first and every later manifest to movie_rating_priority=`letterboxd`;
other household users use `newest_dated`. Policy changes are refused as incidental
batch effects. Ratings retain original score/scale and every revision. Letterboxd
movie ratings win for the configured owner; elsewhere only demonstrably newer
dates supersede current ratings. Overlapping dates/unknown order preserve the
existing selection and report conflicts. Existing ORM rating writers preserve
revisions and honor the same priority; direct SQL rating writers are unsupported.

Deleting a canonical viewing/review unlinks its evidence using PostgreSQL
triggers; original source versions remain. Replaying old evidence does not
resurrect the deleted projection. A changed review stays retained as evidence
after the owner deleted its canonical comment.

## Evidence, privacy and export

All evidence endpoints require a full authenticated user session, never an
arbitrary user ID or a public profile setting. Raw source text is escaped in the
UI. Original UTF-8 JSON/CSV/text files can be uploaded to `/imports/v1/artifacts`
with a verified hash, including unmatched rows. Do not upload credentials or
account/session configuration. Downloads use plain text and `nosniff`.

`GET /imports/v1/export` uses a separate read-only repeatable-read transaction.
It includes all source versions/artifacts, batches, rating revisions/policy,
canonical watches/comments/ratings and referenced media/shows. Decimals and
timestamps are serialized without loss. It does not export account credentials,
connections or playback progress. The original `/export/data` compatibility ZIP
remains available but does not represent the extension's full fidelity. Full
operational recovery uses the consistent PostgreSQL backup plus matching image;
the JSON export is an evidence interchange format, not an automatic DB restore.

History renders date-only values without timezone shifts and labels unknown and
unresolved local times. Separate precision counts are shown in `/imports`.
Existing instant-based time analytics may omit non-instant observations; do not
read those analytics as complete lifetime coverage. Native Nuvio resume and its
15-minute inbound schedules remain independent.

## Development and release

`compose.fidelity-dev.yaml` runs a reusable synthetic PostgreSQL17.11 development
container with no production credentials or Docker socket. Its optional UI is
bound only to localhost17330; database/backend ports are not published.
Its source bind and two named volumes are preserved. It stops after24hours with
no source/test/log activity. Run tests using the image's Python environment:

```
docker exec -e FIDELITY_TEST_DATABASE=synthetic scrob-fidelity-dev /app/backend/.venv/bin/python -m pytest tests/test_fidelity.py -q
docker exec scrob-fidelity-dev /app/backend/.venv/bin/ruff check <touched Python files>
docker exec -w /work/frontend scrob-fidelity-dev npm run build
```

On this Windows workspace, prefix shell commands with `rtk`; use the HomeLab
ComposeGuard for lifecycle actions. Touch `/devstate/activity` for new terminal
test activity. Tests refuse non-synthetic databases. Never run this test suite
against live data. `tools/verify-fidelity-restore.py` verifies a new isolated
synthetic dump/restore and retains its checkpoint; it refuses to overwrite one.

`Dockerfile.household` pins both the upstream omnibus runtime and Node builder
by digest, uses the existing lockfile and adds only patched backend/frontend.
Build after commit with `SOURCE_REVISION` equal to the exact Git revision. Pin
the resulting immutable image in production only after a separate backup/restore
rehearsal and authorization to cut over. No automatic major updates.

Migrations `hfidelity1` and `hfidelity2` are additive descendants of `savedflt`.
Downgrade refuses to discard evidence. Rolling back requires restoring a verified
pre-upgrade database/configuration and the original image together; an old image
cannot simply run over the new Alembic head. Preserve newer evidence before any
rollback. Household operating paths and live deployment acceptance belong in the
HomeLab runbook, outside this source repository.
# Legacy status reconciliation

The private `POST /imports/v1/status-reconciliation/preview` and `/commit`
endpoints deliberately remove redundant imported Stremio status projections
after separately verified Trakt viewing events exist for the same canonical
media. This is not a timestamp or individual-viewing match.

The preview requires an owner-scoped original JSON artifact with format
`scrob-status-projections`, `user_id`, and the exact original `watch_events`
rows; its artifact ID and SHA256, explicit event IDs, source and Trakt account
names, and an idempotency key are mandatory. Only completed, unchanged legacy
Stremio rows with no review, source or active-rewatch references qualify.
Commit uses the preview's batch/manifest/state hashes and rechecks everything
under owner and writer locks. The original row becomes an immutable
`watched_assertion` source version associated with media, with unknown actual
occurrence precision and no arbitrary viewing link. Its original timestamp
and every other field remain in the payload and original artifact. The
receipt distinguishes removed projections from preserved source assertions.

Repeated commits return the original receipt. Playback positions, collections,
ratings, other owners and outbound integrations are untouched. Nuvio inbound
pulls suppress only exact millisecond timestamps archived by a committed
reconciliation, preventing migrated status snapshots from reappearing as new
plays. New timestamps continue to import; unknown timestamps are not suppressed.
The normal evidence/batch/export interfaces include all reconciliation data.
No schema migration is needed beyond `hfidelity2`.
