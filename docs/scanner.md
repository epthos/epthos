notify -> {create, change, move, delete}

```sql
CREATE TABLE Scanner (
    k STRING PRIMARY KEY,
    i INTEGER,
    s STRING,
) STRICT, WITHOUT ROWID;

CREATE TABLE Directory (
    id      INTEGER PRIMARY KEY,
    path    BLOB NOT NULL,     -- in local form, canonicalized
    depth   INTEGER NOT NULL,  -- # of components in the path
    FOREIGN KEY(parent) REFERENCES Directory(id)
        DEFERRABLE INITIALLY DEFERRED,
    
    fastGen INTEGER NOT NULL,
    fullGen INTEGER NOT NULL,
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dir_path ON Directory(Path);

CREATE TABLE FileId (
    id   BLOB PRIMARY KEY,
    path BLOB NOT NULL,
    FOREIGN KEY(parent) REFERENCES Directory(id)
        DEFERRABLE INITIALLY DEFERRED
    
    fastGen INTEGER NOT NULL,
    fullGen INTEGER NOT NULL,
```

Scan can be fast or full. We keep the following state:

TARGET: (fast, fastTime, full, fullTime)
- indicates what files and directories must be checked,
  and when a new full scan is required.

Each directory and file keeps its own clock (fast, full).

For directories, the clock only updates when children are up-to-date,
to help track progress. Overall, fast >= full (a full implies a fast).

We do a fast every time the server starts, as we might have missed
notifications. We do a full every week or so. Note that finding a new
file will trigger a hash check to detect moves, but won't update full
if it's during a fast pass. This ensures that we never have gens larger
than the target.

## API

scanner.Start()
 1. begin notify feed

scanner.SetRoots([dirs])
 1. add missing dirs at (0, 0)
 1. enable scan.

scanner.Candidate() -> struct of what needs scanning:
 - as a stream so that scanner can manage pauses in scanning
 - with a fast vs full bit, to drive the operation
 - we also want to scan full directories, to _find_ contents.

scanner.Update(path: &Path, data):
 - must not assume anything about the origin of the file.

Scanning:
 1. SELECT path FROM Directory 
    WHERE 
      depth = (SELECT MAX(depth) FROM Directory)
      AND gen < $TARGET
    ORDER BY RANDOM() LIMIT 1;
 1. // Walk that directory, add missing subdirectories, handle files.
 1. UPDATE gen = $TARGET WHERE path = $PATH;


TODO: Handling of dropped roots: Crashplan deletes data right away.
      I don't like this as the default, maybe just a future improvement
      to do explicit garbage collection, triggered by the source.

TODO: How do "directory moves" get represented by notify? 
      I need to propagate to all children, incl. keeping FileId.
      Hard to avoid recursive updates in that case, but still rare
      and cheaper than having to re-hash everything to figure out
      identities.


