---
name: Defect or change
about: Anything that changes the product. The compatibility question is not optional.
title: ''
labels: ''
assignees: ''
---

<!--
CLAUDE.md has the full convention. The short version: an issue is read by
whoever picks it up next, possibly three weeks in and new to this
subsystem. Write for them.
-->

## What's wrong, in a sentence

## How you know

<!-- Paste the command and the ACTUAL output. Not a description of the
     output. The output. -->

```
```

## Why it matters

<!-- In terms of something real: data lost, a user seeing the wrong thing,
     an afternoon burned. "Technical debt" is not a reason, it is a way of
     avoiding giving one. -->

## Upgrade and compatibility

<!-- REQUIRED, and required even when the answer is "none" — the answer
     being obvious to you today is not the same as it being recorded.

     Answer three things:

     1. Does this change anything written to disk and read back by a LATER
        binary? The list is .github/persisted-formats.txt — the manifest
        log, the WAL, the object envelope, the config documents, the token
        and principal stores.

     2. If yes: can a version that predates this read the new data and get
        the RIGHT answer? Not "can it parse it". A field an older reader
        silently drops is harmless only if dropping it is harmless; if it
        carries a delete or a drop, ignoring it serves data that was
        removed on request. That is #160, and it went three releases
        unnoticed.

     3. If it cannot: which format version moves, and how does an operator
        get back? "Restore the pre-upgrade backup, there is no in-place
        path" is a complete answer. Silence is not.

     Upgrade is the most common thing an operator does to a database. It
     is worth thirty seconds at the point where somebody still remembers
     the shape of the change. -->

## How you'll know it's fixed

<!-- If you cannot write this down, the ticket is not ready. -->

## The trap

<!-- The obvious-looking fix that does not work, and why. They will find it
     on their own otherwise and lose a day to it. -->
