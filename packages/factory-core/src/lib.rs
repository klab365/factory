//! Domain types for defining and running software-factory pipelines.
//!
//! This crate intentionally has no configuration-format or harness dependencies.
//! TOML loading and agent/shell execution belong in adapters outside the core.

pub mod agent;
pub mod features;
pub mod pipeline;

pub use agent::{AgentDefinition, ModelRef, ModelRefParseError, ThinkingLevel};
pub use pipeline::{HumanOption, Pipeline, PipelineInput, Step, StepKind, Transition};
