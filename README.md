# 1KU Public Qualification

Public, isolated platform-qualification fixture for 1KU architecture decisions.

This repository contains **no 1KU production source, no Vault, no user data, and no real Library database**.

Current test: same-device, same-Library single-Obsidian admission using Node 24 built-in `node:sqlite`.

The fixture verifies:
- same LibraryId: only one independent process may hold the Library gate;
- different LibraryIds: independent;
- same Runtime multiple views: share one gate until the last view closes;
- graceful close and process crash release the gate;
- the gate remains held while the simulated Main DB closes/reopens;
- same LibraryId on different device-local roots does not cross-lock;
- copied 1KU folders with the same LibraryId still contend for the same device-local gate.

A green workflow proves only this isolated primitive on the GitHub-hosted OS runner. It is not Obsidian Host acceptance.
