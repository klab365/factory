/// A named pipeline containing ordered steps and explicit outcome transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    pub name: String,
    pub inputs: Vec<PipelineInput>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineInput {
    pub key: String,
    pub description: String,
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
    Agent {
        agent: String,
        context: Vec<AgentContextInput>,
    },
    Shell {
        command: String,
        args: Vec<String>,
    },
    Human {
        prompt: String,
        options: Vec<HumanOption>,
        approval_keyword: Option<String>,
    },
}

/// A named value made available to an agent from run state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContextInput {
    pub name: String,
    pub source: String,
    pub optional: bool,
}

/// Stable selectable answer offered by a human step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HumanOption {
    pub id: String,
    pub label: String,
}

/// Routes a named step outcome to another step, e.g. `approved` -> `commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub outcome: String,
    pub target_step: String,
}
