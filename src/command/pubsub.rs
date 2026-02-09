// Pub/Sub command handlers
//
// Pub/Sub requires per-connection message channels and is planned for a future
// phase. The SUBSCRIBE/PUBLISH family of commands currently returns an error
// from the dispatch table in command/mod.rs.
