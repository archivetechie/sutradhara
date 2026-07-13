# The life of a shot — a guided tour of the Sutradhara data model

*A companion to [`reference-database-schema.md`](reference-database-schema.md).
The reference is the dictionary: every table, every field, exact and terse.
This is the novel: we follow one evening's footage from the moment a card comes
through the door to the day, years later, when someone reorganizes it into a
collection that never existed before — and along the way we meet every table and,
more importantly, learn **why it earns its place.***

---

## Before we start: the two questions the whole model exists to answer

If you remember nothing else, remember these. Almost every table below is in
service of one of them.

1. **"Are these the same *bytes*, or the same *file*?"**
   Two camera cards can carry a byte-for-byte identical clip. We want to store
   those bytes **once** — but we must never forget that they showed up in two
   different places, at two different times, from two different cards. Content is
   one thing; an *occurrence* of that content is another.

2. **"Is this object on the shelf the same thing as the file I asked for?"**
   To write to tape efficiently we pack hundreds of files into one big archive
   object. But an editor doesn't want "the object" — they want *one 80-gigabyte
   clip* out of it. The thing we stored and the thing someone wants back are not
   the same, and the model keeps them rigorously distinct.

And behind both sits the archive's split personality, which you'll feel
everywhere: it is simultaneously a **preservation institution** ("never lose it,
never quietly change it, prove everything") and a **busy production house**
("give me that clip *now*, and let me reorganize the library whenever I like").
Those two pull in opposite directions. The data model is where they're
reconciled — *archive-first, organize-forever*.

Our cast for the tour:

- **The shoot:** Guru Purnima evening at the Bangalore ashram. Two Sony cameras,
  one audio recorder, and a PDF run-of-show.
- **Ravi**, the videographer, who drives back with the cards (and whose colleague
  in the US sends a card's worth of B-roll over the wire).
- **Meera**, the archivist, who decides how it all should live.
- **The archive** — Sutradhara — the quietly paranoid system that remembers
  everything and trusts nothing it hasn't checked.

Let's open the door.

---

## Chapter 1 — The knock at the door: *receiving*

Ravi hands over the A-CAM card. Meanwhile, over in the US, Ravi's colleague pushes
a card's worth of footage across the internet (this is the receive-over-WAN path —
same front door, just arriving as a stream instead of a physical card). Either
way, the very first thing the archive creates is not a file. It's an **`intake`**.

An **`intake`** is "a batch of stuff arrived." One card offload, one streamed
transfer, one drop-off — one intake. Why make the *batch* a first-class thing
before we even look at the files? Because "a shipment arrived" is a fact worth
tracking on its own: who brought it (`operator`), how it came
(`source_kind` = `card`, `drive`, `upload`, `download`, `handoff`…), which physical
card it was (`card_id`, an opaque identity we'll lean on hard in a moment), and
what *policy class* it belongs to (`artifactclass` — think "these are A-roll
camera masters," which decides everything about how it's later stored).

Two `intake` fields quietly carry enormous weight:

- **`status`** walks `receiving → verifying → registered` (or off to
  `quarantined`). Until it reaches `registered`, nothing inside is trusted
  catalogue truth.
- **`retention_state`** starts at **`held`**. This is the archive promising
  itself: *do not let anyone wipe that card yet.* The card is, right now, the only
  copy of this footage in the universe. We will not release it until we've earned
  the right to. (We come back to this promise in Chapter 8 — it's a whole arc.)

### Why receiving needs so much machinery

Here's the question every newcomer asks: *it's just copying files off a card —
why is there a small government department for it?* Because the card gets erased
and reused. A receive that silently half-finishes, or accidentally runs **twice**,
is either a data-loss event or a data-bloat event — and you won't notice until
it's too late. So three tables stand guard, and they exist entirely to make
"receiving" boringly reliable:

- **`idempotency_record`** — the memory of a request. If Ravi's browser
  double-submits, or a flaky connection makes the client retry, the same
  receive-intent must resolve to the **same** intake, not a second one. The
  `request_hash` catches the nasty case where someone reuses a key but with
  different content ("wait, that's not the same request"). And its
  `duplicate_warning` / `duplicate_acknowledged` fields power a friendly but firm
  workflow: *"We've seen this card before — are you sure?"* — recorded, so the
  override is a decision on the record, not a shrug.
- **`source_claim`** — a **lease on the physical card**. While one receive is
  live, nobody else can start reading the same card out from under it. It carries
  a heartbeat, so if a session dies mid-offload we can *reconcile* the stuck claim
  deliberately rather than guess whether it's safe to steal.
- **`grpc_intake`** — for the streamed workstation/agent path (Ravi's US
  colleague), *this database row*, not the file on disk, is the source of truth
  from first byte to commit. A filesystem marker can be half-written by a crash; a
  committed DB row cannot lie about whether the transfer finished.

And because that streamed path comes from a machine we don't physically control,
two more tables decide *who is even allowed to send*:

- **`grpc_device_enrollment`** binds an mTLS certificate **fingerprint** to a
  device and operator. The identity is the cryptographic fingerprint — a browser
  can't just *claim* to be a trusted workstation by typing its name.
- **`grpc_enroll_token`** is a one-use, short-lived coupon that lets a new device
  request its certificate. Notice the token *is* the primary key: it's a secret
  bearer value, and it may be spent exactly once.

**Takeaway:** none of this stores a single frame of video. It exists so that when
we finally erase that card, we can do it without fear.

---

## Chapter 2 — Same bytes, different stories: *content vs. occurrence*

Now the archive looks at the actual files, and asks Question #1: *are these bytes
new?* This is where the model's first beautiful idea lives, split across two
tables.

A **`logical_asset`** is **one row per distinct byte-sequence**, keyed by its
`content_sha256`. It's the anchor for everything true about *the content itself*:
how big it is, when we first saw it, a coarse `media_kind` (`video`/`audio`/…),
and — critically for a preservation institution — its **doubts**:

- `validity` = `ok` / `suspect` / `unvalidated`: did we actually manage to
  *decode* this, or are we just holding bytes and hoping? An archive that can't
  tell those apart isn't a preservation system; it's a hard drive.
- `rejected_at` / `rejected_by` / `rejection_reason`: an operator can stamp "do
  not use this," and that verdict travels with the content forever.

An **`ingest_item`** is **one *appearance* of that content, inside one intake**.
It remembers the human facts of this specific sighting: the `as_received_path` (the
exact path on the card), a normalized `virtual_path`, even the source filesystem's
`st_dev`/`st_ino` when we can get them. And it records the dedup verdict itself in
`disposition` (`new`, `known_durable`, `known_under_durable`, `reverified`,
`legacy_unknown`) with the evidence that justified it (`disposition_evidence`) and
a pointer to the previous sighting (`prior_intake_id`).

### Why this pays off — watch the B-CAM card

Ravi's B-CAM card was *partially offloaded last week* and re-inserted today. As its
files land, several of them hash to `logical_asset` rows **we already have**. So:

- We do **not** store those bytes again. (Storage saved.)
- But we **do** create fresh `ingest_item` rows for them, because they genuinely
  appeared again, on this card, today. Their `prior_intake_id` points back to last
  week's intake, and their `disposition` says `known_durable` — meaning *"seen
  before, already safely archived,"* so the system can even skip re-archiving them.

That's the whole trick: **deduplicate the bytes, never the provenance.** Two cards
with the same clip cost one copy but keep two histories. (This is also exactly what
makes the tricky Sony "two-events-on-one-card" split safe — the same content can
legitimately appear in two places, and the model can hold that without either
losing a copy or losing track.)

One more small table completes the provenance picture: **`asset_derivation`**. When
we later make a proxy or a transcode, that derived file is its own occurrence, and
an `asset_derivation` edge records *"this occurrence came from that one, via this
`kind` of transform."* Ask "where did this proxy come from?" and there's an answer.

**Takeaway:** *"is a file just a file?"* No. It's *content* (shared, deduplicated,
with its own trust record) and it's an *occurrence* (unique, with its own story).
Keeping them apart is what lets the archive be both frugal and honest.

---

## Chapter 3 — Not everything is welcome: *quarantine and doubt*

One clip on the A-CAM card is corrupt — it won't decode. The archive doesn't
pretend otherwise. That intake (or that item) heads to `quarantined`: the bytes
are **retained as evidence**, but **no trusted `ingest_item` rows are registered**
from it. Nothing broken sneaks into the catalogue wearing a "good file" badge, and
the corresponding `logical_asset.validity` reads `suspect` with a note explaining
why.

This is a small thing that expresses a big principle: *"we have the bytes"* and
*"we have a good file"* are different claims, and a preservation institution must
never blur them.

---

## Chapter 4 — Laying out the exhibit: *arrangement → submission*

The footage is in. Now Meera decides how it should **live** in the archive — what
goes where, under what names. This happens in two moves: a free-editing phase, then
a freeze.

The free phase is an **`arrangement`**: a **mutable workspace** built over one
intake. It has a `status` (`draft → ready → submitted`) and — a lovely touch —
a `cloned_from_arrangement_id`, because the way you *revise* an arrangement is to
**clone it**, never to mutate a committed decision. Inside it, each
**`arrangement_member`** maps one `ingest_item` to a requested `member_path`, with
an `excluded` flag for "actually, leave this one out." Meera drags things around,
renames, excludes the corrupt clip — all cheap, all reversible.

Then she freezes it into a **`submission`**. This is the hinge of the whole
pipeline, so it's worth dwelling on. A submission is an **immutable source-map**: a
fixed list of `submission_member` rows, each pinning an `archive_path`, the
`source_path` it came from, the `sha256` we *expect*, and a stable order (`ord`).
The submission carries a `manifest_digest` — a hash of that whole frozen list.

*Why freeze at all? Why not archive straight from the editable arrangement?*
Because everything downstream — packing bundles, writing tape, counting copies —
is expensive and hard to undo, and it must be **deterministic and auditable**. You
want to shuffle your *intentions* freely right up until the moment of commitment;
and at that moment you want them to become a **signed, unchangeable fact** you can
point at later and say "this, exactly this, is what we set out to store." Want a
different layout? Clone the arrangement and submit again. The frozen record is
never edited — that's what makes it trustworthy.

**Takeaway:** arrangement is *pencil*; submission is *ink*. The model gives you
unlimited pencil and then one honest, permanent line of ink.

---

## Chapter 5 — Where things actually go: *policy, bundles, and the object-vs-file distinction*

This is the heart of the model — Question #2 — so we'll take it slowly.

### First, the rules: policy, backends, and pools

Before a byte is written, policy decides *where copies may go and how many*. Three
tables express this:

- **`artifactclass_policy`** — the compiled rulebook for a class (our A-roll
  masters). It sets when a bundle should flush (`target_bytes`, `max_age_seconds`),
  the restore preference order, and — the crown rule — the **durability floor**:
  `min_copies` and `min_impl_families`, defaulting to **3 copies across 2
  implementation families**. It even records `policy_sha256`, so every decision
  can name *which version of the policy* made it.
- **`backend`** — a concrete storage system: a Remanence tape library, d2tape, an
  SSH disk, S3. Note its `implementation_family` is **derived from `kind`, not
  chosen by a human**. That's deliberate: you cannot *fake* independence.
- **`pool`** — the policy-facing write target *inside* a backend. A backend says
  *how to talk to storage*; a pool says *where policy is allowed to place data*,
  with an `accepts_writes` fence, a `retired` flag, an `offsite_gate`, and a stored
  `representation` (raw bytes, or our RAO object format).

*Why "families" and not just "3 copies"?* Because three tapes of the same format in
the same library are **not** three independent bets — a single format bug or
library failure could take all three. Durability that means anything must span
**independent implementation families** (tape *and* disk *and* cloud), not just
three shelves in the same cupboard. The model enforces the distinction at the
schema level so nobody can quietly satisfy "3 copies" with three fragile ones.

### Now the packing problem: bundles

Here's a practical headache: that PDF run-of-show is 40 KB. Writing a 40 KB object
to LTO tape as its own thing is absurd — tape wants big, streamed writes. So we
**pack**. A **`bundle`** is a *synthetic archive object* that groups many assets
into one write. It has a lifecycle (`open → flushed → sealed`, or `held` for
review) and captures the policy thresholds that govern it. Each
**`bundle_member`** is one asset sitting at a `member_path` inside that object —
and `(bundle_id, member_path)` is unique, because an object can't have two things
at the same path.

Sometimes we transform a member before packing it (a reversible normalization, a
container rewrap). Every such change is logged as a **`staging_transform`**: the
before/after paths, sizes, and hashes, plus a `reversible` flag. The archive will
never quietly alter your bytes and forget it did — if it touched the file, there's
an auditable, often-reversible record of exactly what it did.

### The distinction everything was building toward

We seal the bundle and write it to tape. That written object is a **`copy`** — one
stored realization on one backend. And here's the rule the schema itself calls the
most important modelling choice, enforced by a hard database check: a `copy` names
**either** a single `logical_asset` **or** a `bundle`, **never both**. A bundle
object is not an "individual-file copy," and the model refuses to let anyone
pretend it is. The copy carries what you need to *use* it: a `native_locator` (how
to read/verify/delete it on that backend), an `integrity_hash`, and a `health`
(`ok`/`suspect`/`corrupt`/`missing`) with a `last_verified_at`.

But an editor doesn't want "the bundle object." They want the 80 GB hero clip
*inside* it. So for every asset inside a stored bundle we create an
**`asset_locator`**: it says *"logical asset X lives inside copy Y, at member_path
Z, in representation R."* **That is the bridge between "an object on the shelf" and
"the file a human asked for."** It's how you pull one clip out of a sealed
multi-terabyte tape object without dragging back the whole thing.
(`blob_root` plays a supporting role here, holding root-level metadata for
blob-style bundle storage, kept separate from the per-member locators.)

Now the payoff clicks into place. **Durability is counted over an asset's
`asset_locator`s and any direct `copy`s, across implementation families.** One
bundle written to two independent backends can give *every asset inside it* its
"3 copies / 2 families" — **without pretending each file was written as its own
object.** You get tape-friendly efficiency *and* single-file restore granularity,
and the accounting stays honest because objects and assets are never conflated.

*"Why not just store each file as its own object with three copies and skip all
this?"* Because millions of tiny independent writes would bring a tape library to
its knees, and object stores charge and choke on them too. Bundling makes the write
tractable; the locator layer keeps the restore precise. The `copy`/`bundle`/
`asset_locator` triangle is how you refuse to choose between the two.

**Takeaway:** the shelf holds *objects*; people want *files*; `asset_locator` is
the translator that lets one efficient object serve many precise requests — and lets
"how many copies does this file have?" have a truthful answer.

---

## Chapter 6 — Second opinions: *review and exclusions*

Some bundles shouldn't fan out to tape until a human looks. Such a bundle sits in
`held`, and a person records a **`review_decision`**: an action plus a **`scope`**.
That scope is the clever part — a decision can apply *only to this ingest*, or it
can become a **persisted rule** (with a `subtree` and a stored `persisted_rule`
blob) so the same class of material is handled automatically next time. Reviewers
teach the system once instead of deciding forever.

And whenever material is deliberately *left out* — of a bundle, of a policy result —
an **`exclusion_record`** explains why, durably: a machine-readable `reason`, how
many items and bytes it covered, and which ruleset (`ruleset_hash`) made the call.
So the question *"why isn't this file in the archive?"* always has a recorded
answer, not a mystery.

---

## Chapter 7 — The invisible workforce: *jobs and reconciliation*

You might have pictured all of the above as a script running top to bottom. It
isn't — and that's one of the most important design choices in the system. The
archive is a **reconciler**: it continuously compares *what is* to *what policy
wants*, and does the next needful thing. Three tables run this engine.

- **`job`** is the live work row: a `kind` (which handler), its `params`, the
  `required_resources` it must lease (a tape drive is a scarce, *counted*
  resource), `prerequisites`, and — crucially — `step_state`, a **checkpoint** so a
  job killed mid-flight resumes instead of restarting. A `dedupe_key` guarantees
  the same work isn't queued twice.
- **`job_attempt`** is the **append-only transcript**: every run, its outcome, the
  `granted_leases`, the `worker_id`, even the `code_version` that ran it. It
  survives after the live `job` row is pruned, so history isn't lost to cleanup.
- **`reconciliation_condition`** is the worklist: one row per `(domain,
  target_key)` recording the `observed_state` versus the desired `condition`, the
  `attempt_count`, when it's `next_eligible_at`, and even `blocked_tool_name` /
  `blocked_tool_version` — evidence that lets a stuck condition automatically
  *reopen* once the blocking tool changes.

*Why model work as conditions instead of a pipeline?* Because in the real world
drives are busy, networks drop, and tools have bugs. A straight-line pipeline that
hits a snag just... stops, and someone has to figure out where. A desired-state
reconciler keeps trying, backs off intelligently, self-heals, and — because every
gap is a **row** — can always answer *"why isn't this archived yet?"* with a
specific reason instead of a shrug. (This is hard-won: it's the lesson from a
previous generation of the system that tried to be a pipeline and caught fire.)

**Takeaway:** the archive doesn't *run a script*; it *closes gaps*. That's why it
survives the messy middle and can always explain itself.

---

## Chapter 8 — Safe to wipe the card: *retention and offsite*

Remember Chapter 1's promise — `retention_state = held`, *don't erase that card*?
Now we get to keep it, and only now.

Once the durability floor is truly met (3 copies, 2 families), the archive can move
the intake's `retention_state` from `held → released → purged`, stamping
`released_at` and, when the temporary landing bytes are actually deleted,
`staging_deleted_at`. Each such action is written to an append-only
**`retention_event`** (action, actor, time, evidence) — releasing a card is a
logged decision, not a silent `rm`.

For especially precious material, policy can demand more. A pool's `offsite_gate`
can require an **`offsite_confirmation`** — an operator's attestation, with a
`shipment_id`, that a physical medium has actually left the building — *before*
landing bytes are released. The card is the last unique copy until the archive can
prove it has enough *independent, and possibly offsite,* copies to not need it.

*"Couldn't we just delete after the first tape write succeeds?"* No — and the model
is built to refuse. It will not forget the source until it can **prove** it doesn't
need it. `retention_state` is the archive saying, out loud and on the record: *"I
am now safe. You may reclaim the card."*

**Takeaway:** the scariest button in any archive is "erase the original." This whole
chapter exists so that button is only ever pressed against proof.

---

## Chapter 9 — Organize forever: *virtual arrangements*

Fast-forward a year. Meera wants to build a collection: **"Guru Purnima, across the
years."** The relevant footage is scattered across tapes, packed inside bundles
from a dozen different events. Does she rewrite tape to gather it? Absolutely not —
and here the archive's *organize-forever* half finally gets its moment.

A **`virtual_arrangement`** is a **permanently mutable, catalogue-only view**. It
moves **no bytes**. It's a way of *naming and grouping* archived material that lives
entirely in the database. Its **`virtual_arrangement_member`** rows point at assets
by `(logical_asset_hash, artifactclass)` — that pair is the durable identity — while
the `path` is free to change. And every path change is written to an append-only
**`virtual_arrangement_history`** (old path, new path, actor), because reorganizing
is a *first-class, audited activity*, not a destructive rename. Add
**`asset_tag`**s (soft-deletable, so removal history survives) and you can slice the
library by theme, by speaker, by year — endlessly.

This is the archive's dual identity made concrete, and it's genuinely elegant:

- The **tape layer** is frozen truth. Written once, verified, never casually
  touched. *Archive-first.*
- The **virtual layer** is infinitely rearrangeable. Build, rename, merge, and
  retire collections forever, with a full audit trail, and **not one tape ever
  spins to do it.** *Organize-forever.*

*"Why not just re-archive everything into the new structure?"* Because rewriting
tape for every reorganization is slow, expensive, and risky — and it would make the
library afraid of its own catalogue. By separating the **namespace** (endlessly
mutable, cheap, in the database) from the **bytes** (write-once, precious, on the
shelf), the archive can be reshuffled like index cards while the shelves never move.

**Takeaway:** the shelves are sacred and still; the catalogue dances. That gap is
the single most important thing the archive *is*.

---

## Chapter 10 — Getting it back, fast: *restore and the HD cache*

Next week an editor needs the hero clip. This is the production house talking, and
speed matters, so the model tracks a restore carefully.

A **`restore_request`** is the persisted operator ask, with a `state`
(`pending → active → completed`, or `completed_with_errors`) and an
`admitted_by` / `admitted_capabilities` record — because *who is allowed to pull
this, especially if it's private material* is checked and logged at admission.
Idempotency fields stop an anxious double-click from spawning two restores. Each
**`restore_request_item`** tracks one asset through its own little journey:
`queued → waking_disk → streaming → done`, or `fell_back_to_tape`, or `denied`
(with a `denial_kind` like `capability` or `privacy_unmapped`), and it reports
`bytes_restored` so a UI can show real progress.

Notice `waking_disk` and "fell back to tape." That's because there's a speed layer:
the **HD cache**. A **`cache_disk`** is an enrolled fast disk; a **`cache_entry`**
is one asset sitting on it, hot and ready. And the model is emphatic about one
thing: **the cache is expendable.** These tables **deliberately do not** make a
cache disk a durable `backend` or `pool`. A `cache_entry` has a `trusted` flag and
can be `lost`; if a cache disk dies (`state = dead`), **nothing durable is lost** —
it simply repopulates from tape. The restore tracks its cache branch and its tape
branch independently, so a cache miss transparently falls back to the real,
durable source.

*"Isn't a hot cache basically an extra copy — doesn't it count toward durability?"*
No, and the model keeps that line bright on purpose. **Tape (and its independent
families) is truth; the cache is speed.** Blur them and one day someone trusts a
cache disk as a backup and loses data. So the schema won't let you.

**Takeaway:** the cache makes the archive *fast*; the durability rules make it
*safe*; and the model never lets you mistake one for the other.

---

## Closing: the two questions, one last time

We started with two questions. Now they have faces:

1. **Same bytes or same file?** → `logical_asset` (the content, deduplicated and
   honest about its own validity) vs. `ingest_item` (each occurrence, with its own
   provenance). Store once; remember every appearance.

2. **The object on the shelf, or the file I want?** → `bundle`/`copy` (the efficient
   stored object) vs. `asset_locator` (the precise pointer to one file inside it).
   Pack for the machine; restore for the human; count copies honestly.

Around those sit the guardrails: the receive tables that make erasing a card safe,
the arrangement→submission freeze that turns intention into an auditable fact, the
policy/pool/family rules that make "durable" mean something, the job/reconciliation
engine that closes gaps and explains itself, the retention arc that never forgets
the source until it can prove it's safe, and the virtual-arrangement layer that lets
the library be reorganized forever without disturbing a single tape.

And the small print in the reference — the `copy` asset-or-bundle XOR, the unique
path constraints, the partial unique indexes for live jobs and active tags — isn't
decoration. Those are the model refusing, at the database level, to let anyone tell
a comfortable lie about what the archive knows.

That's the whole cast. When you next open
[`reference-database-schema.md`](reference-database-schema.md) and see a terse row
like `asset_locator.copy_id → copy.id`, you'll know it isn't bookkeeping — it's the
bridge that lets one tape object hand back exactly the clip someone asked for, a
decade from now.

---

### Where to look next (chapter → reference section)

| If you followed… | The exact fields live in `reference-database-schema.md` under… |
|---|---|
| Receiving (Ch. 1) | *Content and intake* (`intake`), *Receive API and device relay* |
| Content vs. occurrence (Ch. 2–3) | *Content and intake* (`logical_asset`, `ingest_item`, `asset_derivation`) |
| Arrangement & freeze (Ch. 4) | *Arrangement and submission* |
| Policy, bundles, the triangle (Ch. 5) | *Storage policy and archive objects* |
| Review & exclusions (Ch. 6) | `review_decision`, `exclusion_record` |
| Jobs & reconciliation (Ch. 7) | *Jobs and reconciliation* |
| Retention & offsite (Ch. 8) | `offsite_confirmation`, `retention_event`, `intake.retention_state` |
| Virtual arrangements (Ch. 9) | *Organization, retention, and review* |
| Restore & cache (Ch. 10) | *HD cache and restore* |
