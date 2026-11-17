# Pipeline-Konfiguration

- `agents/` enthält wiederverwendbare Agentendefinitionen.
- `pipelines/feature.toml` beschreibt den Build-/Review-/Test-Ablauf.
- Jeder Pipeline-Run erzeugt unter `runs/<run-id>/` genau eine Markdown-Datei pro eindeutigem Agenten der Pipeline, z. B. `00-coder.md` und `01-reviewer.md`.
- Der Auftrag wird in jede Agenten-Datei kopiert. Derselbe Agent verwendet seine Datei bei späteren Steps des Runs weiter; neue Runs erhalten neue Dateien.
- Run-Dateien werden nicht versioniert. Keine Geheimnisse in ihnen ablegen.

Agentenschritte können über die CLI erst ausgeführt werden, wenn ein Pi-Agenten-Runner implementiert ist. Bis dahin unterstützt die CLI nur Shell-only-Pipelines.
