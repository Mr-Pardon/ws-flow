# Client 1.x archive

The previous handwritten Client implementation is preserved here as migration reference.
It is outside the active build, has no production exports, and retains its original
`ws-event-proxy` based protocol/watch model. Its archived package.json records former
dependencies, not a publishable package layout. The original full-project backup also
remains in the sibling ws-flow-backups directory.

Client 2.x lives in packages/client/src and uses the shared RuntimeCore. See
[the migration guide](../../docs/manual/reference/migration.md) for API and lifecycle changes.
