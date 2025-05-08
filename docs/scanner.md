Instead of having an immutable ID to represent a file, we could use the id
as the HKDF parameter. Then we can more easily have multiple files
share the salt when we realize they are actually related, and would benefit
from block sharing. This still makes attacks hard as the salt can only be
influenced by having direct access to the existing files. We can also stop
caring about collisions, as long as we generate randomly for the other files.

Generate the id/salt only after the first hash? This would help distinguish
new files from already seen files, and ensure we back up only after we know
we won't duplicate blocks unnecessarily.

Instead of forwarding the watcher's content directly as filepicker.next(),
why not store the new file size and change time directly into filestore ?
Introduce a dirty bit, so that the file is flagged for backup.

File states:

- NEW: no assigned id yet, only size & mtime. Triggers hashing & matching.
- DIRTY: caused by a change in size, mtime or hash. Triggers backup.
- BUSY: actively being backed up. Should we hardlink to snapshot?
- CLEAN: caused by backup completion. Provides updated size, mtime, hash as
         of the backup. 

<O> -- Scan::update(mtime, fsize) --> NEW
    -- mark_dirty(mtime, fsize)   --> NEW

NEW   -- hash_next(hash) --> DIRTY, add encryption_group.
DIRTY -- backup_start    --> BUSY, snapshot.
BUSY  -- complete(mtime, fsize, hash) --> CLEAN
CLEAN -- hash_next(hash), scan::update() --> DIRTY if changed.
UNAVAILABLE -- file can't be read


TODO: Handling of dropped roots: Crashplan deletes data right away.
      I don't like this as the default, maybe just a future improvement
      to do *explicit* garbage collection, triggered by the source.

TODO: How do "directory moves" get represented by notify? 
      I need to propagate to all children, incl. keeping FileId.
      Hard to avoid recursive updates in that case, but still rare
      and cheaper than having to re-hash everything to figure out
      identities.


