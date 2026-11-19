# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/klab365/factory/compare/v0.1.0...v0.1.1) - 2026-11-19

### Added

- add configurable pipeline inputs and issue intake
- persist and resume pipeline runs
- enforce structured agent handoffs
- snapshot pipeline config per run
- add isolated Pi agent runner

### Fixed

- remove fixed review cycle limit
- hand off step results and bound review loops

### Other

- Persist shared pipeline handoff history
- Add cancellable pipeline runs
- Add configuration validation and pipeline planning commands
- Configure explicit agent pipeline context
- Add resumable human pipeline steps
- Fix CLI pipeline exit codes
- Handle repeated reviewer feedback in feature runs
- Require pipeline inputs and avoid task prompt
- update version of repo
- Initial software factory pipeline implementation
- Fix CI lint and pipeline validation
- Fix CI clippy warning
- Have feature pipeline publish commits and PRs
- add release workflows and name binary factory
