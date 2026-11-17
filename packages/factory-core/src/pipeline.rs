/// A named pipeline containing ordered steps and explicit outcome transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub id: String,
    pub kind: StepKind,
    pub transitions: Vec<Transition>,
}

/// The executor selected for a step. Concrete execution lives in adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepKind {
    Agent { agent: String },
    Shell { command: String, args: Vec<String> },
}

/// Routes a named step outcome to another step, e.g. `approved` -> `commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub outcome: String,
    pub target_step: String,
}
