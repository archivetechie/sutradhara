# How Material Enters the Archive, Gets Organized, and Lives Forever
### A plain-language guide to intake, arrangement, and the long life of a file

> This is the companion guide to the technical design `design-arrangement-arc.md`.
> Same ideas, no code — written for archivists, operators, and anyone who wants to
> understand *why* the system works the way it does. If you ever wonder "why can't I
> just drag the files into folders like normal?", the answer is in here.

---

## The one big idea

A preservation archive has to do two jobs that pull in opposite directions.

The first job is **preservation**: the moment footage arrives, get it safe — checksummed, copied to tape, copied (encrypted) to a second server on the LAN — fast, mechanically, before anything can go wrong. A camera card is a single fragile copy; every hour it stays that way is risk.

The second job is **organization**: deciding what the material *is* — which programme, which day, which session, what it should be called, who can see it. That work is slow, human, and never really finished. Footage gets re-catalogued years later as understanding changes.

The mistake almost every system makes is to **couple these two clocks**: footage waits, as a single risky copy, until a human finishes organizing it. The scarce, slow resource (human attention) holds the urgent, mechanical one (preservation) hostage.

This system's central move is to **decouple them**:

> **Preserve at machine speed from first contact. Organize whenever, forever — as a layer of catalog metadata that never touches the preserved bytes.**

Everything that follows is a consequence of taking that idea seriously.

---

## A story: a morning shoot, from card to catalogue

Before the concepts, here's the whole journey concretely. Imagine an operator comes back from a morning shoot with a camera card holding a few clips: `A001.MOV`, `A002.MOV`, and so on.

**1. It is received.** The operator runs the receive step, which copies the card to the archive's "landing" area, **computing a fingerprint (a checksum) of every file as it reads it**. It writes those fingerprints into a small standard manifest alongside the copied files, and drops a "this is complete" marker last. From this instant there are two verified copies of the footage (the card and the landing copy), and a tamper-evident record of exactly what came in. The card can now be wiped with confidence.

The copied tree is **evidence**. Nobody will ever rename, move, or delete anything inside it. (Hold that thought — it's the load-bearing rule.)

**2. It is inspected.** Before the archive *commits* to anything, it looks — read-only. It re-computes the fingerprints and checks them against the manifest, confirms nothing is missing or extra, validates the metadata. The footage either passes ("valid") or is set aside ("quarantined"). Inspection changes nothing; it just reports.

**3. It is registered.** This is the moment of **acceptance** — the archive formally takes custody. Each received file becomes a catalogued item with an identity, a fingerprint, a recorded origin (which card, which shoot, where it sat in the received tree), and a default name. At the same time, the encrypted disaster-recovery copy — to a second server on the LAN — is made automatically — because you want a second safe copy as soon as possible, not after a human gets around to it.

**4. Review copies are prepared.** Nobody sorts 4K masters by streaming them over the network from a cheap laptop. So the system makes **proxies** — small, smooth preview versions — plus any other derived material the policy asks for (a fast-scrub preview, an HD editing proxy, a searchable index). The operator asks for this with a single request naming a *profile* (e.g. "hd-review"); the system figures out which jobs that profile implies.

**5. A human arranges it.** Now the operator opens a workspace that looks like an ordinary folder full of those lightweight proxy files, and sorts them in Finder or Explorer exactly as they always have — into `satsang/day-1/…`, renaming, grouping. Crucially, **they are arranging the proxies, and the system is quietly recording their layout as intentions** — "the operator wants this master to live at `satsang/day-1/A001`." The masters never move. The evidence tree never moves.

**6. The layout is submitted (frozen).** When the rough arrangement is done, the operator submits it. The system freezes the human layout into a simple, immutable **map**: *"the archive entry `satsang/day-1/A001.MOV` is made from the original file `…/landing/…/A001.MOV`, fingerprint abc123."*

**7. It is archived.** The archive writer reads that map and **streams the original bytes straight to tape under the chosen names** — checking the fingerprint as it goes. Note what does *not* happen: it does **not** first copy terabytes of 4K into a nicely-named folder tree just to have something to archive. The arrangement was a list of pointers; archiving follows the pointers.

**8. It is organized forever.** Months later, someone reorganizes — moves things into `/programs/satsang/…`, adds a tag like `kailash`, marks something "rejected." All of this edits a **virtual layer** of names and tags in the catalogue. **The tape never moves. Nothing is ever deleted.** A "reject" is a marker, not an erasure. When someone restores a file — by its new virtual name or its old one — the catalogue resolves the name back to the physical bytes on tape.

That's the whole arc. The rest of this guide explains *why* each part is shaped the way it is.

---

## The ideas underneath

### 1. The evidence is sacred — you organize a *layer over* it, never the thing itself

The single most important rule: **what arrives is immutable evidence.** The received tree (a standard "BagIt" bag — payload files plus a checksum manifest) is provenance. It is never the place where humans sort, rename, or tidy.

Why so strict? Three reasons:

- **Trust and provenance.** In a 30-year archive you must always be able to say, with cryptographic certainty, "this is exactly what came off that card, untouched." The moment operators start dragging the original files around, that guarantee is gone.
- **Tape can't be un-written.** Once material is sealed onto a tape, you do not rewrite the tape to reflect a new folder structure — that would mean re-spooling miles of tape every time someone renames a folder. So organization *must* live somewhere editable (the catalogue), separate from the sealed bytes.
- **Organization changes; evidence doesn't.** The same clip might be filed three different ways over the years. If "filing" meant moving files, you'd be corrupting your evidence every time you changed your mind.

So the system keeps a hard line: **the bag is read-only forever; all human organization is recorded as catalogue entries that point *at* the bag.** Arrangement and later re-organization are *namespaces over the catalogued material*, never edits to the received bytes.

### 2. Two kinds of identity: the bytes, and the occurrence

The catalogue tracks two different things that are easy to confuse:

- **The bytes** (in the design: a *LogicalAsset*) — one entry per unique chunk of content, identified by its fingerprint. If the exact same file arrives from two different cards, it's the *same bytes*, recorded once.
- **The occurrence** (an *IngestItem*) — one entry per *received instance* of a file. It points at the bytes but carries the facts that are true of *this arrival*: which intake it came in, where it sat in the received tree, what it's named in the human layout, what tags it has, what its archival class is.

Why split them? Because the same bytes can legitimately mean different things in different contexts. A piece of black filler might appear in two shoots; it's one set of bytes (store/verify it once) but two occurrences (two places in two layouts, possibly different tags). Identity-of-content and facts-of-this-arrival are genuinely different questions, so they get different homes. This is the same reason a library has one catalogue record for a *work* but a separate card for each *copy* on each shelf.

### 3. Why the journey is a series of small, named, trustworthy steps

An earlier version of this system had one big command — "scan" — that quietly did everything: checked the files, committed them to the catalogue, kicked off expensive video processing, and started preparing them for archive. That's convenient right up until it isn't. An operator running a quick "let me just check these files" had no way to know it would also commit catalogue records and spin up hours of conversion. **A command you can't predict is a command you can't trust.**

So the journey is deliberately broken into distinct steps, each with one clear job and honest, predictable side effects:

- **Inspect** — *look, don't touch.* Validate the bag and report. Creates nothing. Safe to run anytime.
- **Register** — *take custody.* The explicit moment the archive accepts the material into its catalogue. This is where records are born.
- **Prepare** — *ask for the working material.* Request the proxies/indexes/derived work (by naming a profile).
- **Arrange** — *lay it out.* Let a human organize, over cheap review copies.
- **Submit** — *freeze the layout* into the map that drives archiving.
- **Archive** — *write to tape* under the submitted names.
- **Virtual segregation** — *organize forever*, after archive, in catalogue metadata only.

The value isn't bureaucracy; it's **predictability**. Each step has a name, does exactly what its name says, and can be repeated safely. "Inspect" never has surprise costs. "Register" is the one place custody begins. An operator always knows what a command will and won't do.

(A convenience command can still bundle the common case — "accept this and prepare review proxies" in one go — but it's a shorthand built *on top of* the honest steps, not a return to the magic do-everything button.)

### 4. Proxies, and the "recipe" that stays open to new kinds of work

You don't sort 4K masters over the network. You make **proxies** — lightweight preview copies — and sort *those*. More generally, after registration the system produces **derivatives**: a fast preview for scrubbing, an HD editing proxy, a partial-restore index, and — over time — things nobody's asked for yet: audio extractions, transcriptions, thumbnails, scene detection.

The tempting mistake is to hard-code "make a proxy" into the pipeline. Then every new kind of derived work means surgery on the core system. Instead, the system uses a **recipe** (in the design: a *prepare profile*):

> A profile is just a configurable list: *"for this kind of material, under this profile, produce these derived things."* For example: *masters + "hd-review" → an editing proxy, a preview, and a partial-restore index.*

Adding a brand-new kind of derived work next year — say, transcription — is **a line of configuration plus a small handler**, not a new command and not a change to the pipeline. The operator's request never changes ("prepare, hd-review"); the *recipe* changes. This is a recurring theme: **the system is fixed about its core nouns and open-ended about the work that produces them.**

One subtle but important detail: **each derived thing is a first-class item with its own filing class.** A proxy isn't "a cheap shadow of the master" — it's its own catalogued item that belongs to a *proxy* class, which says "keep 1 copy." The master belongs to a *masters* class, which says "keep 3 copies, one offsite, one encrypted." So a proxy is preserved like anything else — just to its own, cheaper, durability rule. (And the system must never accidentally let a proxy inherit the *master's* expensive 3-copy rule; the proxy's class comes from the recipe, deliberately.) The model even recurses: a derivative could itself have derivatives. Everything is "an item with a class," and the class decides how many copies it gets — **including zero.** Not every derived thing is preserved with its own copies: a partial-restore index just *rides along* on its source; the disaster-recovery copy (a second LAN server) is deliberately *temporary* (kept until the tape copies are confirmed, then deleted); a cheap thumbnail might be kept only while convenient and simply *regenerated* from the master if it's ever lost. "Don't really preserve this" is just a class with no permanent copies — said in the same vocabulary as everything else. (And one more quiet benefit of this design: the small bit of code that *makes* a new kind of derivative — a transcription job, say — only has to speak in plain terms, "I produced a transcript from this master." It never needs to know how the catalogue stores things. That keeps adding new kinds of work genuinely easy.)

### 5. Arrange over copies, freeze a map, archive without wasteful copies

This is the most elegant part of the design, and it mirrors how a film editor actually works.

An editor doesn't cut the original negative to try out an edit. They arrange *proxies* on a timeline — a list of pointers — and only when the cut is locked does the lab conform the *actual* negative to that list. The negative is touched once, at the end, exactly as the locked list dictates.

The archive does the same:

- **Arrange:** the operator organizes the cheap proxy files in a normal file manager. The system watches and records each move/rename as an **intention** — "this master should be archived at this path." Folders, renames, groupings — all captured as catalogue rows. The masters and the evidence bag don't budge.
- **Submit:** the arrangement is frozen into a **map** — for each archive entry, the chosen name, the original source file it's made from, and the expected fingerprint. Think of it as a packing list: *"shelf `satsang/day-1/A001.MOV` ← original box `…/A001.MOV`."*
- **Archive:** the writer reads the map and **streams the original bytes to tape under the chosen names**, verifying fingerprints as it goes.

The payoff is huge. Naively, to archive a nicely-named folder tree, you'd first *build* that tree — copying terabytes of 4K masters into a second location just so there's a tidy directory to feed the tape. That's a waste of time, space, and I/O on the most expensive files in the building. By keeping the arrangement as **pointers** and archiving **through the map**, the masters are read exactly once — straight from where they already are — and written under whatever names the human chose. **Arrange freely; copy nothing extra.**

### 6. Organize forever: the catalogue is editable, the tape is not

After material is on tape, organization doesn't stop — but it changes character. Now it's purely a matter of **virtual names and tags** in the catalogue:

- Move something from `/as-received/DCIM/A001.MOV` to `/programs/satsang/day-1/A001.MOV` — that's a catalogue edit, with full history (who, when, from where, to where). The tape object never moves.
- Add a tag like `kailash` — a label that other policies can key on later (access groups, restore approvals). Tags attach to material; the filing class stays a purely *archival* grouping.
- Mark something "rejected" — and here's the principle: **nothing is ever deleted.** "Reject" is a marker that hides an item from default views; the bits stay safely archived. This is the **archive-everything** rule: bad, rejected, or hard-to-decode material is *preserved and flagged*, never silently culled. Storage is cheap; a deletion you regret in ten years is not recoverable.

When someone restores a file — by its current virtual name, an old name, or a tag — the catalogue resolves that name through the item's identity down to the physical location on tape. Names are a flexible surface; the bytes underneath are fixed.

### 7. Getting it back out — and a clever shortcut for giant files

Storing is only half the job; eventually someone wants material *back*. Restore is where the catalogue earns its keep: you ask for something by its current name (or an old name, or a tag), the catalogue resolves that down to the exact spot on tape, and the bytes come back — verified against their fingerprint on the way out.

Most of the time this is simple: pull one file out of an archived bundle. The bundle carries a little table of "this file lives at these bytes," so the drive seeks straight to it and reads just that file — no need to read the whole bundle.

The interesting case is **pulling a short clip out of an enormous master** — say 30 seconds out of a 2-hour 4K file that's a quarter of a *terabyte*. Restoring the whole 250 GB just to grab 30 seconds is wasteful: on tape that's ~11 minutes of reading to use 0.1% of it. So for these giants we can be cleverer — but only because of a quirk of how tape works.

On tape, **moving to a position ("seeking") and reading data are completely different speeds.** Seeking shuttles the tape at high speed, reading nothing — crossing the *entire* tape takes about a minute. Actually *reading* the entire tape would take about twelve *hours*. So seeking past data is hundreds of times faster than reading it. And here's the key: restoring the whole file and restoring just the clip pay the **same** seek to reach that part of the tape; the only difference is how much they then *read*. Whole-file reads 250 GB (~11 minutes); the clip reads ~1 GB (a few seconds). Same seek, vastly less reading — about a **13× faster** restore, and the drive is freed up 13× sooner for the next request.

The catch is that this only pays off for the *giants*. Every restore has a fixed overhead of about a minute (mounting and seeking) that the shortcut can't beat — so it's only worth it when the reading you avoid is much bigger than that minute: files of roughly a hundred gigabytes and up. For an ordinary few-GB file, just restore the whole thing and trim the clip; the shortcut would save seconds and isn't worth the extra machinery. So the rule is simple: **whole-file for everything normal; the byte-range shortcut only for the handful of enormous high-bitrate masters** — which, conveniently, is exactly where most of the archive's bytes live. (And even on the shortcut, the actual video-cutting is handed to the standard tool, ffmpeg; we never reinvent that part.)

### 8. The tireless librarian: how work actually gets decided

So far we've described *what* should happen. The interesting question is *how the system makes sure it happens* — reliably, at the scale of tens of millions of files, for decades.

The naive approach is "do X when Y happens": *when* a master is registered, kick off a proxy job. The problem is that **a missed moment means the work silently never happens.** A process crashes at the wrong instant, a job is added after the trigger already fired, a replay goes wrong — and a file quietly never gets its proxy, and nobody notices until someone needs it years later.

So the system uses a sturdier idea, borrowed from how robust infrastructure works (and from a hard lesson on a previous system that leaned on triggers and lost work). Picture a **tireless librarian** who doesn't act on announcements but instead walks the shelves with a checklist:

> *"For everything that should have copies — does it? For everything that should have a proxy — does it? No? Then make the missing ones."*

The librarian constantly compares **what should exist** (the desired state, computed from policy and the operator's requests) against **what does exist** (the catalogue facts), and closes the gaps. Crucially, the decision to do work is always made from **current reality**, not from a stale announcement. If the work was already done, the librarian moves on. If it was somehow missed, the *next* pass catches it. The system **self-heals** and converges on the desired state no matter what went wrong.

There's one practical wrinkle worth understanding, because it's the difference between a design that works on paper and one that works at scale. You can't have the librarian literally re-examine all fifty million items on every pass — that would grind the whole system to a halt. So announcements aren't *thrown away*; they're turned into a **to-do list of "things that might have changed and are worth re-checking."** The librarian works that list (cheap), and a slower, periodic **full sweep** runs in the background as a safety net to catch anything the to-do list missed. Announcements make it *fast*; the sweep makes it *correct*. Neither alone is enough.

This same librarian handles *every* kind of "should exist" the same way: copies, proxies, indexes, freshness re-checks ("this copy hasn't been verified in 180 days — re-verify it"). And it composes with the recipes from idea #4: a new kind of derived work is a new recipe entry, and the librarian starts ensuring it exists — no new machinery. (One more nuance: when work genuinely *can't* succeed right now — a corrupt source, a tool that needs upgrading — the librarian marks it "blocked" and stops banging on it, until the thing that would fix it actually changes. It retries when there's a reason to, not on a timer.)

### 9. The sealed crate: when a "file" is really a folder of a million files

A last wrinkle that matters in real media work. Some things that *look* like a single file are actually folders containing thousands-to-millions of internal pieces — a Final Cut Pro library (`.fcpbundle`), a Photos library, an app bundle. The operator thinks of it as one item; the operating system treats it as one item; but on disk it's an explosion of tiny files.

If the archive naively walked *into* such a folder, it would catalogue a million entries, build a million-line manifest, copy a million files into the arrangement projection, and re-walk them at every later stage. Madness.

So the system treats these as **one object from the very first moment of contact**. When the receive step meets one of these packages, it doesn't go inside; it **seals the whole thing into a single bundle, computes one fingerprint for it, and treats it as one item end to end** — one catalogued item, one thing to arrange, one object on tape. It still has to *read* every internal byte once (to seal it), but after that it's **one object forever** — no million-entry explosion downstream. A small internal index is kept alongside, so you can still pull a single internal file back out later if you ever need to.

The principle: *walk once, then one object forever.* (And because sealing must be perfectly reproducible — the fingerprint is the package's identity — the exact sealing recipe is pinned and versioned, so the same library always seals to the same bytes on any machine, this year and in ten years.)

---

## The principles that tie it all together

If you remember nothing else, remember these:

1. **Preserve fast, organize forever.** Safety happens at machine speed from first contact; organization is a slow human layer that never blocks preservation.
2. **The evidence is immutable.** What arrived is never edited. All organization points *at* it.
3. **The catalogue is the single source of truth, and it's editable; the tape is sealed.** Names and structure live in the catalogue; bytes live, unmovably, on tape.
4. **Archive-everything; flag, never cull.** Nothing is silently deleted. "Rejected" is a label, not an erasure.
5. **Steps are small, named, and predictable.** You always know what a command will and won't do. Custody begins at exactly one place.
6. **Configuration over code.** New kinds of derived work, new policies, new recipes are configuration plus a small handler — not surgery on the pipeline. And that handler speaks in plain domain terms ("I made a transcript from this master"); it never needs to know the database's internals.
7. **Desired-state, not triggers.** The system continuously closes the gap between what should exist and what does, so it self-heals and never silently loses work.
8. **Arrange with pointers; copy nothing extra.** Human layout is a map; archiving follows the map and reads each original exactly once.

---

## A short glossary

- **Intake / the bag** — one received batch of material, copied to the landing area with a checksum manifest. Immutable evidence.
- **Fingerprint (checksum / SHA-256)** — a short code derived from a file's bytes; changes completely if even one byte changes. The basis of all integrity checks.
- **LogicalAsset (the bytes) / IngestItem (the occurrence)** — content-identity vs facts-of-this-arrival. Same bytes from two cards = one LogicalAsset, two IngestItems.
- **Artifactclass (filing class)** — the policy label that decides how an item is preserved: how many copies, where, encrypted or not. "masters" = 3 copies incl. an offsite encrypted one; "proxy" = 1 copy.
- **Derivative** — anything produced *from* a registered item: an editing proxy, a preview, an index, a transcript. Each is its own item with its own filing class.
- **Prepare profile (the recipe)** — configuration that says "for this kind of material under this profile, produce these derivatives." New kinds = a recipe line, not new code.
- **Proxy** — a lightweight preview/editing copy used so humans can sort and review without touching 4K masters.
- **Projection** — the temporary, normal-looking folder of proxy files an operator arranges in Finder/Explorer; the system records their moves as intentions.
- **Source-map (the frozen layout)** — the immutable list submitted before archiving: "this archive name ← this original file, this fingerprint." Archiving streams originals through it; no wasteful staging copy.
- **Virtual segregation** — post-archive organization: editing virtual names and tags in the catalogue. The tape never moves; nothing is deleted.
- **Reconciler (the tireless librarian)** — the background process that continuously compares "what should exist" to "what does exist" and closes the gap. Self-healing; the reason work is never silently lost.
- **Package normalization** — sealing a folder-that-is-really-one-thing (a Final Cut library) into a single object at first contact, so it doesn't explode into a million catalogue entries.

---

*Want the engineering detail behind any of this — the exact records, the commands, the failure modes? That all lives in `design-arrangement-arc.md`. This guide is the "why"; that one is the "how."*
