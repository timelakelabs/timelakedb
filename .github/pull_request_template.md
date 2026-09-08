<!--
The sections below are the ones CLAUDE.md asks for. Delete the ones that
genuinely do not apply; a heading with nothing under it is worse than no
heading.

Compat is the one that is CHECKED. If this touches a file in
.github/persisted-formats.txt, CI reads the line below and cross-checks it
against the diff.
-->

Compat: none

<!--
  none      nothing persisted changed shape.
  additive  an older binary still reads new data CORRECTLY.

            Careful. A field an older reader silently drops is additive
            only if dropping it is HARMLESS. If it carries an instruction —
            a delete, a drop, a retirement — then an older reader ignoring
            it does the wrong thing quietly, and that is `breaking` however
            optional the field looks. Three releases of manifest fields were
            waved through as additive on exactly that reasoning (#160).

  breaking  it does not. Bump the format version in the same change, and add:

              Downgrade: <how an operator gets back, or that they cannot>

            That line matters because this pull request is the only place
            that still knows how the new shape maps onto the old one.
-->

## What changed, and why

## The risky part

<!-- You know which hunk you would want a second pair of eyes on. Naming it
     is not weakness; making a reviewer hunt for it is how it gets
     rubber-stamped. -->

## Verified

<!-- The actual output. Test counts, the drill that went red then green.
     "I tested it" is not evidence and should not read as if it were. -->

## Blast radius

<!-- What else touches this, and what breaks if it is wrong. If the answer
     is genuinely nothing, say how you know. -->

## What I left out, and why

## What I am not sure about
