# The life of a shot — a guided tour of the Sutradhara data model

*A companion to the field-level references ([`reference-database-schema.md`](reference-database-schema.md)
for the byte/occurrence/storage/view layers, and the data-model & Artifact
architecture write-up in `~/system/docs/` for the Artifact, membership,
policy-version, bundle-part, and evidence layers). Those are the dictionaries:
every table, every field, exact and terse. This is the novel: we follow one
evening's footage from the moment a card comes through the door to the day, years
later, when someone reorganizes it into a collection that never existed before —
and along the way we meet every entity and, more importantly, learn **why it earns
its place.***

---

## Before we start: a model made of *identities*

The whole data model is really a chain of **identities**, each answering a
different question, each stored exactly once at its own natural grain. Read the
chain top to bottom and you've basically got the system:

> **bytes → occurrence → the thing we mean to keep → draft namespace → frozen
> promise → packed object → physical realization → mutable view**

or, in table names:

> `logical_asset → ingest_item → artifact → arrangement → submission →
> bundle → copy`, with `intake` as the receive envelope and
> `virtual_arrangement` as the post-archive view.

The reason the model looks "big" at first is that these really are *different*
things, with different lifespans and different rules — and the whole craft is to
represent each concept **once**, at the grain where it's actually true. If you
remember three distinctions, the rest falls into place:

1. **Same *bytes* or same *file*?** Two cards can carry a byte-for-byte identical
   clip. We store those bytes once, but never forget they showed up in two places.
   *Content* is one thing; an *occurrence* of it is another.

2. **The stuff that *arrived*, or the thing we *mean to keep*?** A card is a
   custody event. "The Guru Purnima masters" is a business object an operator will
   prepare, archive, and ask for by name for the next thirty years. Those aren't
   the same, and — this is the grain we most recently realized was missing — the
   business object needs its own identity: the **`artifact`**.

3. **The object on the *shelf*, or the *file* I asked for?** To write tape
   efficiently we pack hundreds of files into one big object. But an editor wants
   *one 80-gigabyte clip* out of it. The thing we stored and the thing someone
   wants back are not the same, and the model keeps them rigorously distinct.

And behind all three sits the archive's split personality, which you'll feel
everywhere: it is at once a **preservation institution** ("never lose it, never
quietly change it, prove everything") and a **busy production house** ("give me
that clip *now*, and let me reorganize the library whenever I like"). Those pull
in opposite directions. The data model is where they're reconciled —
*archive-first, organize-forever*.

Our cast:

- **The shoot:** Guru Purnima evening at the Bangalore ashram. Two Sony cameras,
  one audio recorder, and a PDF run-of-show.
- **Ravi**, the videographer, who drives back with the cards (and whose colleague
  in the US sends a card's worth of B-roll over the wire).
- **Meera**, the archivist, who decides what should be kept and how.
- **The archive** — Sutradhara — quietly paranoid, remembers everything, trusts
  nothing it hasn't checked.

Let's open the door.

---

## Chapter 1 — The knock at the door: *the intake*

Ravi hands over the A-CAM card; his US colleague pushes a card's worth of footage
across the internet. Either way, the first thing the archive creates isn't a file.
It's an **`intake`** — the record of *a custody event*: a batch of material
arrived and we are now responsible for it.

Why make the *arrival* a first-class thing before we even look at the files?
Because "a shipment came in, from this source, in this operator's hands" is a fact
worth pinning on its own: who brought it (`operator`), how it came (`source_kind` =
`card`, `drive`, `upload`, `download`…), which physical card it was (`card_id`, an
opaque identity we'll lean on hard in a moment), and a `retention_state` that
starts at **`held`** — the archive promising itself *do not let anyone wipe that
card yet*. Right now that card may be the only copy of this footage in existence.

A subtle but important point: an intake is a **custody boundary, not a policy
boundary.** One card can carry two completely different events; a single event can
arrive on three cards. So an intake carries an `artifactclass` only as a
*receive-time default* — a hint — not as the final word on "what is this and how
must it be stored." (That authority will live somewhere better; hold the thought.)

### Why receiving needs a small government department

Newcomers always ask: *it's just copying files off a card — why the ceremony?*
Because the card gets erased and reused. A receive that silently half-finishes, or
accidentally runs **twice**, is either a data-loss event or a data-bloat event —
and you won't notice until it's too late. So three tables stand guard, existing
entirely to make "receiving" boringly reliable:

- **`idempotency_record`** — the memory of a request. If Ravi's browser
  double-submits, or a flaky link makes the client retry, the same receive-intent
  must resolve to the **same** intake, not a second one. Its `request_hash` catches
  the nasty case of a reused key with different content, and its
  `duplicate_warning` / `duplicate_acknowledged` fields power a friendly-but-firm
  *"we've seen this card before — are you sure?"* — recorded, so an override is a
  decision on the books, not a shrug.
- **`source_claim`** — a **lease on the physical card**, so two operators can't
  read it at once. It heartbeats, so a session that dies mid-offload can be
  *reconciled* deliberately rather than guessed away.
- **`grpc_intake`** — for the streamed workstation/agent path (Ravi's US
  colleague), *this database row*, not the file on disk, is the authority from
  first byte to commit. A filesystem marker can be left half-written by a crash; a
  committed row cannot lie about whether the transfer finished.

And because that streamed path comes from a machine we don't physically control,
two more tables decide *who may even send*: **`grpc_device_enrollment`** binds an
mTLS certificate **fingerprint** to a device and operator (a browser can't just
*claim* to be trusted by typing a name), and **`grpc_enroll_token`** is a one-use,
short-lived coupon for a new device's certificate request — the token *is* its own
primary key, because it's a secret bearer value spent exactly once.

**Takeaway:** none of this stores a single frame. It exists so that when we finally
erase that card, we can do it without fear.

---

## Chapter 2 — Same bytes, different stories: *content vs. occurrence*

Now the archive looks at the files and asks the first deep question — *are these
bytes new?* — and answers it with two tables that must never be confused.

A **`logical_asset`** is **one row per distinct byte-sequence**, keyed by its
`content_sha256`. It anchors everything true about *the content itself*: its size,
when we first saw it, a coarse `media_kind`, and — because a preservation system
must be honest — its **doubts**: `validity` (`ok` / `suspect` / `unvalidated`: did
we actually manage to *decode* this, or are we just holding hopeful bytes?) and the
`rejected_*` markers an operator can stamp to say "do not use this."

An **`ingest_item`** is **one *appearance* of that content, inside one intake**. It
remembers the human facts of this specific sighting: the `as_received_path` (exactly
where it sat on the card), the source filesystem's device/inode when we can get it,
and the dedup verdict itself in `disposition` (`new`, `known_durable`,
`known_under_durable`, …) with the evidence behind it and a `prior_intake_id`
pointing at the previous sighting.

### Why this separation pays for itself

Ravi's B-CAM card was *partly offloaded last week* and re-inserted today. As its
files land, several hash to `logical_asset` rows **we already have**. So:

- We do **not** store those bytes again. (Storage saved.)
- We **do** create fresh `ingest_item` rows, because they genuinely appeared again,
  on this card, today — with `prior_intake_id` pointing back and `disposition` =
  `known_durable`, meaning *"seen before, already safely archived."*

That's the whole trick: **deduplicate the bytes, never the provenance.** And a
sharper rule falls straight out of it: **hash equality proves byte equality, not
sameness of purpose.** Two occurrences of the identical clip stay two
`ingest_item`s and are **never automatically merged** — because the same footage
might legitimately belong to two different events. (This is also exactly what makes
the tricky Sony "two-events-on-one-card" split safe.)

One more small table completes the provenance picture: **`asset_derivation`**.
When we later make a proxy or transcode, that derived file is *its own occurrence*,
and an `asset_derivation` edge records *"this came from that, via this kind of
transform."* Ask "where did this proxy come from?" and there's an answer.

**Takeaway:** *"is a file just a file?"* No. It's *content* (shared, deduplicated,
honest about its own validity) and it's an *occurrence* (unique, with its own
story). Keeping them apart is what lets the archive be both frugal and truthful.

---

## Chapter 3 — The thing we actually mean to keep: *the artifact*

Now Meera says the most natural sentence in the world: *"archive the Guru Purnima
masters."* And here's the puzzle that motivated the newest grain in the model: **in
the database so far, there's no single row that *is* "the Guru Purnima masters."**

It isn't the `intake` (that card also had unrelated B-roll on it; the event
actually spans two cards). It isn't a `logical_asset` (that's one clip's bytes). It
isn't the arrangement or submission we haven't made yet. It certainly isn't the
tape object. The thing everyone *talks about* — the bounded, meaningful object you
prepare, archive, browse, and ask for by name — had no home. So it gets one: the
**`artifact`**.

An **`artifact`** is the **stable, policy-bearing identity of one intentionally
bounded archival object.** Usually that's an event/camera folder or a whole card;
it can also be a single file, a deliberately chosen group of loose files, or a
normalized directory package (its `kind` records which). It is the operator's unit
for preparation, arrangement, archive-compliance, browse, and restore. It is **not**
a storage object — that distinction is the point of the whole next few chapters.

The fields are chosen so the *identity* survives everything that legitimately
changes around it:

- Its `id` is a plain **UUID**, deliberately meaningless. *Why not the content
  hash?* Because identical bytes are **not** the same business object — two events
  could share a stock clip. And *why not derive it from the folder path or the
  time?* Because you rename folders, correct boundaries, and re-organize; identity
  must not leak from any of that. A stable, opaque id is exactly what lets the
  Guru Purnima artifact stay "the same thing" through a relabel, a membership fix,
  or byte-deduplication.
- `label` is a human name you're free to change — *not* identity.
- `artifactclass_id` is the **single current authority** for "what class is this,
  and therefore how must it be validated, placed, and retained." (Remember the
  intake only held a *default* — this is where the real policy identity now lives.)
- `source_root`, `kind`, `boundary_basis` describe how the boundary was drawn.
- `definition_sha256` is a digest over the artifact's whole confirmed definition —
  its identity, intake, class, members, and each member's relative path — so the
  boundary itself is a signed, checkable fact.

Which occurrences belong to the artifact is recorded by **`artifact_member`** rows —
one per `ingest_item`, each with a path *relative to the artifact boundary*. Two
rules make this trustworthy:

- **The membership rows are authoritative, not the folder layout.** A card's
  top-level directories are a hint, not a decree.
- **An occurrence is a current primary member of exactly one artifact** (history is
  kept, never erased). And derivatives (proxies, transcodes) are **never** primary
  members — they hang off via `asset_derivation` and merely *appear* in the
  artifact's detail view. Primary membership means "original evidence," and only
  originals count.

### Where the boundary comes from — seeded at receive, drawn at the organizing table

Drawing the boundary is deliberately *not* done at the door. At receive, the
system only **seeds a proposal** from whatever intent the gesture carried —
"this selected folder," "this whole card" — a cheap, freely-editable draft that
makes no promises. The real decision happens where the operator can actually
*see* the material: in the arrangement session (the arranger's proxy
projections, reorganized with ordinary Finder gestures), where carving the top
level of the workspace *is* drawing the artifact boundaries. The **submit**
gesture then does both solemn acts in one signature — it confirms the boundary
(freezing primary membership, writing durable definition evidence) and freezes
the arrangement. Routine cards whose structure is already right skip the
session entirely with a one-click "archive as-is."

*Why not just auto-adopt whatever folders are on the card?* Because an arbitrary
top-level directory is a **guess**, and a wrong boundary that gets baked in and
lives for decades is exactly the mistake you can't cheaply undo. Proposals cost
nothing to redraw; confirmed boundaries cost successor artifacts and audit
trail. So the system keeps everything in pencil until the one moment the
operator commits — and loose files are never silently dropped; mixed media
kinds are allowed only if the class policy says so. The model would rather ask
than assume.

### Artifacts live for decades — so they have lineage

Real archival objects get corrected, split, combined, and re-received across years.
The model never handles that by rewriting the past. Instead, **`artifact_relation`**
edges record `split_from`, `merged_from`, `reingest_of`, and `supersedes` between
artifact identities; **`artifact_event`** is an append-only audit stream (confirmed,
relabeled, reclassified, membership-ended, retired); and
**`artifact_external_identifier`** carries outside IDs (a legacy D2 accession, say)
without ever overloading the artifact's own id. Corrections make **successors**;
they don't edit history.

**Takeaway:** the artifact is the word the operator was already using. Giving it a
stable identity — separate from the card it came on, the bytes inside it, and the
tape it lands on — is what lets a human say "the Guru Purnima masters" and have the
whole system know exactly, and durably, what they mean.

---

## Chapter 4 — One source of truth: *class and policy authority*

Notice a trap the model deliberately avoids. "What class is this, and how must it be
stored?" is a question that *could* be answered on the intake, the occurrence, the
arrangement, the submission, and the bundle — and if all of them claimed to be the
answer, they'd drift and you'd never know which to believe.

So the model names exactly one boss and demotes the rest to honest snapshots:

- **`artifact.artifactclass_id`** is the **current desired policy** — the single
  authority.
- The intake's and occurrence's class are **as-received defaults** — what someone
  typed at the door, kept for reference, not obeyed as policy.
- The submission's and bundle's class come with an **immutable
  `policy_version_id`** — a snapshot of *the policy as it stood when we froze or
  sealed*, so old physical facts are never relabeled by a later rule change.

That last piece leans on **`artifactclass_policy_version`**: policies are
**versioned and immutable**, with one active version at a time. The live
compliance check always reads the artifact's *current desired* policy, while every
submission and bundle keeps the exact version it was born under.

*Why bother?* Because an archive's rules evolve over decades, and you must be able
to change tomorrow's policy **without rewriting yesterday's history.** One current
authority for decisions, plus immutable snapshots for the record — that's how you
get both.

**Takeaway:** the same fact repeated in five places isn't five facts, it's four
future contradictions. One authority, many dated snapshots.

---

## Chapter 5 — Draft, then freeze: *arrangement → submission*

Now Meera lays out how the Guru Purnima artifact should live in the archive
namespace — and she does it in two moves: free editing, then a freeze.

The free move is an **`arrangement`**: a **mutable naming/organization revision of
exactly one artifact** (that scoping matters — you're arranging *a business object*,
not a random pile of intake items). Each **`arrangement_member`** points at an
**`artifact_member`** and proposes a `member_path`. Meera drags, renames,
reshuffles the folder layout — all cheap, all reversible. This is *pencil*. (The
corrupt clip she worried about isn't here at all — it was quarantined at
registration, before it could ever become a member; Chapter 6 tells that story.
An arrangement can rename members but never drop them, because everything
confirmed gets archived.)

Then she **freezes** it into a **`submission`** — *ink*. A submission is an
**immutable source-map**: it captures the artifact's id and definition digest, its
intake, its class and exact `policy_version_id`, and a fixed list of
**`submission_member`** rows. Each frozen member pins the occurrence, the
`logical_asset_hash` we expect, the source path, the artifact-relative path, the
submitted path, and — importantly — a **`disposition`**:

- `materialize` — write this one now;
- `satisfied_existing` — already durably archived (from a prior receive), so it need
  not be re-written.

There are deliberately only these two. "Archive everything" is absolute here (a
decision made explicitly, 2026-07-14): policy may never exclude a primary member,
so every member of a confirmed artifact must carry one of these two dispositions —
a card's stray junk files get archived with the card. Anything genuinely unwanted
has to be dealt with *before* the boundary is confirmed (quarantine, or don't
confirm), where the omission stays loud instead of becoming a silent gap.

*Why record `satisfied_existing` instead of just omitting the member?* Because a
missing row is **ambiguous** when you later rebuild the catalogue from evidence — did
we forget it, or decide it? A frozen submission must be able to say *why every
confirmed member was or wasn't written.* Silence is not allowed.

*And why freeze at all — why not archive straight from the editable arrangement?*
Because everything downstream — packing, tape writes, copy-counting — is expensive
and hard to undo, and must be **deterministic and auditable**. You want to shuffle
your *intentions* freely right up to the moment of commitment; and at that moment
you want them to become a **signed, unchangeable promise**. Want a different layout
later? Make a *new* revision (submissions relate to their predecessors); you never
edit a frozen one.

**Takeaway:** arrangement is pencil, submission is ink — and the ink even records
the files it chose *not* to write, so the story is never ambiguous.

---

## Chapter 6 — Not everything is welcome: *quarantine and doubt*

One clip on the A-CAM card won't decode. The archive doesn't pretend otherwise: it
heads to `quarantined` — the bytes are **kept as evidence**, but **no trusted
occurrence is registered** and no artifact claims it, and the corresponding
`logical_asset.validity` reads `suspect` with a note. A small mechanism expressing a
big principle: *"we have the bytes"* and *"we have a good file"* are different
claims, and a preservation institution must never blur them.

---

## Chapter 7 — Where things actually go: *policy, bundles, and the object-vs-file distinction*

This is the heart of the model — the third deep question — so we'll go slowly.

### First, the placement rules

Before a byte is written, policy decides *where copies may go and how many*:

- **`backend`** — a concrete storage system (a Remanence tape library, d2tape, an
  SSH disk, S3). Its `implementation_family` is **derived from `kind`, not chosen by
  a human** — you cannot *fake* independence — and its `tier` records whether the
  backend is `self_describing` (we can rebuild the catalogue by reading it) or
  `catalog_authoritative` (its database backup matters more).
- **`pool`** — the policy-facing write target *inside* a backend, with an
  `accepts_writes` fence, a `retired` flag, an `offsite_gate`, and a stored
  `representation` (raw bytes, or our RAO object format).
- The **durability floor**, carried by the class policy: `min_copies` and
  `min_impl_families`, defaulting to **3 copies across 2 implementation families.**

*Why "families" and not just "3 copies"?* Because three tapes of the same format in
the same library aren't three independent bets — one format bug or library failure
could take all three. Durability that means anything spans **independent families**
(tape *and* disk *and* cloud), not three shelves in one cupboard. The model enforces
the distinction so nobody can quietly satisfy "3 copies" with three fragile ones.

### The packing problem: bundles

That PDF run-of-show is 40 KB. Writing it to LTO tape as its own object is absurd —
tape wants big, streamed writes. So we **pack**. A **`bundle`** is a *synthetic
archive object* that groups many files into one write. It has a lifecycle
(`open → sealed`), carries an immutable `policy_version_id`, and — crucially — is
**self-describing**: every bundle embeds a reserved manifest
(`_sutradhara/bundle-manifest.cbor`) listing exactly what's inside. Each
**`bundle_member`** is one file at a `member_path`; sometimes we transform a member
before packing (a reversible container rewrap), and every such change is logged as a
**`staging_transform`** with before/after paths, sizes, hashes, and a `reversible`
flag — the archive never quietly alters your bytes and forgets it did.

### The object is not the file (and the box is not the event)

We seal the bundle and write it to tape. That written object is a **`copy`** — one
physical realization on one backend. A hard rule keeps it honest: a `copy` names
**either** a single `logical_asset` **or** a `bundle`, **never both** — a bundle
object is not "an individual-file copy," and the model refuses to pretend it is. The
copy carries what you need to *use* it: a `native_locator` (how to read/verify/delete
it), an `integrity_hash`, and a `health` (`ok`/`suspect`/`corrupt`/`missing`).

But an editor doesn't want "the bundle object." They want the 80 GB hero clip
*inside* it. So for every asset inside a stored bundle there's an **`asset_locator`**
saying *"logical asset X lives inside copy Y, at member-path Z, in representation
R."* **That is the bridge between an object on the shelf and the file a human asked
for** — how you pull one clip out of a sealed multi-terabyte tape object without
dragging back the whole thing.

### The box is a *many-to-many* with the event — and we say so honestly

Here's where the artifact grain earns its keep physically. The relationship between
**artifact** and **bundle** is genuinely many-to-many:

- several small artifacts may share **one** bundle (efficient packing);
- one oversized artifact may be split across **several** bundles;
- a later re-submission may add new bundles without touching old ones.

A bare join can't express that safely, so the model qualifies it with
**`artifact_bundle_part`**: it records *which immutable submission contributed which
part to which bundle*, with `part_number` / `part_count` so a split artifact's plan
is provably **complete and contiguous**. Inside a shared bundle, each artifact's
files live under a reserved namespace (`artifacts/<artifact-id>/…`) so paths can't
collide, and restore simply strips the prefix. And each `bundle_member` links back
to its `submission_member`, so you can prove the *exact* occurrence-and-path
provenance of every packed byte.

*Why all this bookkeeping?* Because the questions "which event's files are in this
box?" and "is this box, plus its siblings, the **complete** set for that event?"
must have provable answers — otherwise compliance, restore, and disaster-rebuild are
all guessing.

Now the payoff clicks: **durability is counted over an asset's `asset_locator`s and
any direct `copy`s, across implementation families.** One bundle written to two
independent backends gives *every asset inside it* its "3 copies / 2 families" —
**without pretending each file was written as its own object.** Tape-friendly
efficiency *and* single-file restore granularity, with honest accounting, because
objects and assets are never conflated.

**Takeaway:** the shelf holds *objects*, people want *files*, and events get packed
across *boxes*. `asset_locator` and `artifact_bundle_part` are the two translators
that keep all of that both efficient and truthful.

---

## Chapter 8 — "Is it safely archived?" is *computed*, not flagged

You might expect an artifact to have a big `status` column: `archived`, `pending`,
and so on. It deliberately does **not**. Instead, its state is **projected** — computed
on demand from the authoritative records — along several **independent axes**:
custody, temporary-protection, preparation, arrangement, materialization, durability,
retention, and an *attention* list of typed blockers with a suggested next action.

*Why axes instead of one status enum?* Because a single status **hides independent
failures** (a thing can be perfectly arranged but under-replicated) and inevitably
drifts into a second, competing workflow engine out of sync with the real jobs,
copies, and retention truth. Compute the truth from the evidence; don't store a
summary that can lie.

The compliance computation, in plain terms: every primary member must have a
disposition; every `materialize` member must have a matching bundle member with the
right hash; a split artifact's bundle parts must all be present and contiguous; and
you count only **distinct, verified** copies that satisfy the family/offsite rules.
A shared-bundle failure makes **every** artifact in that box non-compliant.

And one distinction the UI must never fumble, because it's the difference between a
to-do and a disaster:

- **Not archived** — nothing was ever submitted for this requirement (a workflow
  gap).
- **Under-replicated** — some valid copies exist, but not enough for the floor.
- **Missing** — a copy we *recorded as present* can't be verified at its locator (a
  real loss).

Never label an unsubmitted artifact "missing"; and never delete a copy row to make a
lost tape disappear — keep the row and mark its health, so the loss stays visible.

**Takeaway:** the archive would rather *recompute* whether you're safe than *trust a
flag* that says you are.

---

## Chapter 9 — The invisible workforce: *jobs and reconciliation*

None of the above runs as a top-to-bottom script. The archive is a **reconciler**:
it continuously compares *what is* to *what policy wants* and does the next needful
thing. Three tables run it.

- **`job`** — the live work row: a `kind`, its `params`, the `required_resources` it
  must lease (a tape drive is a scarce, *counted* resource), and `step_state`, a
  **checkpoint** so a job killed mid-flight resumes instead of restarting. A
  `dedupe_key` guarantees the same work isn't queued twice.
- **`job_attempt`** — the **append-only transcript**: every run, its outcome, the
  leases it held, the worker, even the `code_version`. It survives after the live job
  row is pruned.
- **`reconciliation_condition`** — the worklist: one row per `(domain, target_key)`
  recording `observed_state` versus desired `condition`, a retry count, and even the
  `blocked_tool` evidence that lets a stuck condition *reopen itself* once the
  blocking tool is fixed.

*Why model work as conditions instead of a pipeline?* Because drives are busy,
networks drop, and tools have bugs. A straight pipeline that hits a snag just stops,
and a human has to find where. A desired-state reconciler keeps trying, backs off,
self-heals, and — because every gap is a **row** — can always answer *"why isn't this
archived yet?"* with a specific reason. (Note the deliberate line from Chapter 8:
job execution is **orthogonal** to what a thing *is*. Work is never promoted into a
lifecycle status; that's exactly the drift the projected-state design refuses.)

**Takeaway:** the archive doesn't *run a script*; it *closes gaps* — and can always
explain the ones still open.

---

## Chapter 10 — Safe to wipe the card: *retention and offsite*

Remember Chapter 1's promise — `retention_state = held`, *don't erase that card*?
Now we earn the right to.

Once the durability floor is truly met, the intake's `retention_state` moves
`held → released → purged`, and every such action is written to an append-only
**`retention_event`** — releasing a card is a logged decision, not a silent `rm`. For
especially precious material, a pool's `offsite_gate` can require an
**`offsite_confirmation`** (an operator's attestation, with a shipment reference, that
a physical medium has actually left the building) *before* release.

Retention is deliberately scoped to the **intake**, because the staging bytes and the
temporary encrypted backup are intake-scoped. That has a very human consequence worth
stating plainly: **one non-compliant artifact holds the whole intake.** If the Guru
Purnima masters are safe but the B-roll on the same card isn't, the card isn't
released yet — and the UI shows *per-artifact* blockers alongside the shared hold, so
it's obvious *why*. After the staging bytes are purged, everything that matters —
artifacts, membership, occurrences, submissions, bundles, copies, policy snapshots,
hashes, receipts, relations, retention events — is preserved. Only the ephemeral bytes
and rebuildable caches go.

*Couldn't we just delete after the first tape write succeeds?* No — the model is built
to refuse. It will not forget the source until it can **prove** it doesn't need it.

**Takeaway:** the scariest button in any archive is "erase the original." This whole
chapter exists so it's only ever pressed against proof.

---

## Chapter 11 — Never forget how to rebuild: *the deepest reason for the shape*

Here is the founding rule that quietly explains half the design decisions above:
**losing the database must not lose the archive.** The catalogue is meant to be a
*rebuildable index* over self-describing storage.

But some of what we've created can't be recomputed from bytes. You can re-derive a
file's hash and a copy's location by reading a tape. You **cannot** re-derive "these
particular files are the Guru Purnima masters, in class X, drawn at this boundary,
split into these parts" — those are **human decisions**. So the artifact layer
**cannot live only in SQLite.**

The model solves this with two durable homes for exactly the non-derivable facts:

- a small, append-only **Catalog Evidence Journal** holding only the decisions that
  can't be inferred — artifact confirmation and definition, class/policy versions,
  split/merge/re-ingest/supersede relations, submission freezes, bundle seals,
  retention authorizations, offsite/loss attestations — each document canonical,
  content-hashed, and written to **at least two independent durable locations before
  the matching destructive step**;
- the **self-describing bundle manifest** embedded in every bundle
  (`_sutradhara/bundle-manifest.cbor`), naming the artifacts, members, submissions,
  and hashes inside it — so the object on the shelf carries its own meaning, in a
  format that works for RAO, plain tar/object storage, and the D2 tape adapter alike.

A disaster rebuild then reads like a recipe: enumerate every backend, open each
bundle's manifest, verify the hashes, recreate the policy versions, then the
artifacts, occurrences, submissions, bundle-parts, copies, and locators — and
**flag conflicts for a human rather than guess.** (One quiet nuance: a Remanence
object's `caller_object_id` is set to the **bundle** id, because one object is one
bundle and may hold several artifacts — the truth lives in the embedded manifest, not
in a single backend field.)

**Takeaway:** the reason the model insists on stable ids, immutable snapshots, signed
definitions, and self-describing objects is this one promise — *you could burn the
database to the ground and rebuild the archive's meaning from the shelves.*

---

## Chapter 12 — Organize forever, and restore by the *thing*

Fast-forward a year. Meera wants a collection: **"Guru Purnima, across the years,"**
gathering footage scattered across tapes and bundles from a dozen events. Does she
rewrite tape? Absolutely not — and here the *organize-forever* half of the archive
finally takes its bow.

A **`virtual_arrangement`** is a **permanently mutable, catalogue-only view.** It
moves **no bytes.** Its **`virtual_arrangement_member`** rows point at assets by
identity while the `path` is free to change, every change audited in an append-only
**`virtual_arrangement_history`**, and **`asset_tag`**s (soft-deletable, so removal
history survives) let her slice the library by theme, speaker, or year — endlessly. A
virtual arrangement is *only ever a view*: it never becomes source provenance or
policy authority.

This is the archive's dual identity made concrete:

- The **tape layer** is frozen truth — written once, verified, never casually
  touched. *Archive-first.*
- The **virtual layer** is infinitely rearrangeable — build, rename, merge, and
  retire collections forever, with a full audit trail, and **not one tape ever
  spins.** *Organize-forever.*

And when an editor finally needs the footage back, they ask for **the artifact** —
"restore the Guru Purnima masters" — and the system resolves the current (or a chosen)
submission, restores **all** its parts, strips the reserved bundle namespace, and
hands back the event with its provenance intact. That request is tracked in a
**`restore_request`** (with authorization captured at admission) and per-asset
**`restore_request_item`** rows that walk `queued → waking_disk → streaming → done`,
or honestly report `fell_back_to_tape`, `denied`, or `failed`.

Behind it sits a speed layer — the HD cache (**`cache_disk`**, **`cache_entry`**) —
about which the model is emphatic: **it is expendable.** These tables *deliberately*
are not durable backends or pools; a cache disk can die (`dead`) and **nothing
durable is lost** — it repopulates from tape. Tape is truth; the cache is speed; and
the schema keeps that line bright so no one ever mistakes a hot cache for a backup.

**Takeaway:** the shelves are sacred and still; the catalogue dances; and you can
always ask for your footage back *as the thing you meant*, not as a pile of anonymous
objects.

---

## Closing: fewest *concepts*, each at its natural grain

We started with a chain of identities and three questions. Now they have faces:

1. **Same bytes or same file?** → `logical_asset` (content, deduplicated, honest
   about its validity) vs. `ingest_item` (each occurrence, with its provenance).
2. **The stuff that arrived, or the thing we mean to keep?** → `intake` (a custody
   event) vs. `artifact` (the bounded business object, with stable identity, one
   policy authority, and durable definition evidence).
3. **The object on the shelf, or the file I asked for?** → `bundle`/`copy` (the
   efficient stored object) vs. `asset_locator` (the precise pointer to one file
   inside it), with `artifact_bundle_part` keeping the event↔box relationship honest.

Around them sit the guardrails: the receive tables that make erasing a card safe, the
one-authority-plus-dated-snapshots discipline for policy, the arrangement→submission
freeze that turns intention into a signed promise (recording even what it *didn't*
write), the placement rules that make "durable" mean something, the projected state
that recomputes safety instead of trusting a flag, the reconciler that closes gaps and
explains itself, the retention arc that never forgets the source until it can prove
it's safe, the evidence journal and self-describing bundles that let you rebuild the
archive's *meaning* from the shelves alone, and the virtual layer that lets the library
be reorganized forever without disturbing a single tape.

The guiding idea behind all of it is worth saying outright: **the best model is not the
one with the fewest tables — it's the one with the fewest independent *concepts*, each
represented exactly once, at the grain where it's actually true.** Adding the artifact
wasn't adding a table for its own sake; it was giving a name we were already using a
home it never had — and, in doing so, letting several other things (policy authority,
the box-vs-event relationship, the rebuild story) finally sit at their natural grain
too.

So when you next open the field references and see a terse line like
`asset_locator.copy_id → copy.id` or `artifact_bundle_part.submission_id`, you'll know
it isn't bookkeeping. It's the machinery that lets a human say *"give me the Guru
Purnima masters"* — and get them back, whole and provably theirs, a decade from now.
