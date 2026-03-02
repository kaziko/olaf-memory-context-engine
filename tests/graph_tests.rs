// Graph store CRUD tests live as inline #[cfg(test)] unit tests inside
// src/graph/store.rs — keeping them there avoids widening the store API
// to pub just to satisfy an external test crate.
//
// This file is reserved for future integration-level graph tests that
// verify behaviour across multiple graph subsystem components.
