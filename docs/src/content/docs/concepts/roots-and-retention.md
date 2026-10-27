---
title: Roots & Retention
description: What keeps an object graph available and how roots change safely.
---

A `RootName` is a validated hierarchical name pointing to one `ObjectKey` in
one repository. Setting or replacing a root commits a new repository revision.
The root retains its target and every object reachable through verified
forward links.

Removing a root makes that graph eligible for collection if no other root or
active hold still needs it. Shared blobs and chunks remain while another live
graph references them. Roots are permanent by default. On a local repository,
an application can mark a root evictable for disk-pressure collection, which
releases least recently used evictable roots when space is low. This is useful
for rebuildable artifacts; Git histories and releases can remain permanent.
The policy is repository metadata and does not change the root record's frozen
encoding.

## Active operations also retain data

Mutation sessions protect staged payloads until publication finishes or the
session ends. A retention hold keeps the snapshot it read usable while other
writers and collection proceed. Collection preserves these pinned lifetimes
while removing unrelated garbage.

A snapshot hold covers records created through its metadata generation. It
does not retain unrelated records published later. Creation generations are
repository bookkeeping, not part of an object's content identity. See
[Garbage Collection](../garbage-collection/) for the full collection sequence.

## Conditional publication

Scripts can use `root set NAME TARGET --if-absent` to claim an unregistered
name, or `root set NAME TARGET --if-target KEY` to replace a value they read
earlier. A failed condition changes neither the root nor its retention policy
and exits nonzero. See [`root set`](../../reference/cli/#root-set).

`MutationSession::publish_if_roots_match` can publish staged objects and root
changes only while every watched name still has its expected target, including
expected absence. An unrelated state change is retried; a watched name change
returns `RootMismatch` without publishing those logical changes.

This protects a service from overwriting a root that changed since it was read.
The service still needs to control who may write its names. The check compares
the current target, not the name's full history: after removal, a delayed
create may succeed again. To reject that replay, advance an owner fence root
in the same commit. See [Share an S3 Repository Across Owners](../../guides/s3-multi-owner/)
for an example.

Roots and revisions have meaning within one repository. `sync --root` installs
the selected source value at the destination after verification; it does not
order concurrent changes across repositories. Applications needing a merge
policy must provide one.
