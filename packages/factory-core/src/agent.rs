use std::{error::Error, fmt};

/// A reusable logical agent definition. Harness-specific execution is handled by an adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDefinition {
    pub name: String,
    pub harness: String,
    pub model: ModelRef,
    pub thinking_level: Option<ThinkingLevel>,
    pub prompt: String,
}

/// A model identifier split into its provider and provider-specific model name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
}

impl ModelRef {
    /// Parses a `provider/model` identifier, splitting at the first slash.
    pub fn parse(value: &str) -> Result<Self, ModelRefParseError> {
        let (provider, model) = value
            .split_once('/')
            .ok_or(ModelRefParseError::MissingSeparator)?;

        if provider.trim().is_empty() || model.trim().is_empty() {
            return Err(ModelRefParseError::EmptyPart);
        }

        Ok(Self {
            provider: provider.to_owned(),
            model: model.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingLevel {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRefParseError {
    MissingSeparator,
    EmptyPart,
}

impl fmt::Display for ModelRefParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSeparator => write!(f, "model must use the provider/model format"),
            Self::EmptyPart => write!(f, "provider and model must both be non-empty"),
        }
    }
}

impl Error for ModelRefParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_provider_and_model() {
        let model = ModelRef::parse("anthropic/claude-sonnet-4").unwrap();
        assert_eq!(model.provider, "anthropic");
        assert_eq!(model.model, "claude-sonnet-4");
    }

    #[test]
    fn rejects_invalid_model_identifiers() {
        assert_eq!(
            ModelRef::parse("claude-sonnet-4"),
            Err(ModelRefParseError::MissingSeparator)
        );
        assert_eq!(
            ModelRef::parse("anthropic/"),
            Err(ModelRefParseError::EmptyPart)
        );
    }
}
