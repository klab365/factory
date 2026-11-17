# Pipeline-Konfiguration

- `agents/` enthält wiederverwendbare Agentendefinitionen.
- `pipelines/feature.toml` beschreibt den Build-/Review-/Test-Ablauf.
- Jeder Pipeline-Run erzeugt unter `runs/<run-id>/` einen `config.json`-Snapshot und genau eine Markdown-Datei pro eindeutigem Agenten der Pipeline, z. B. `00-coder.md` und `01-reviewer.md`.
- Der Auftrag wird in jede Agenten-Datei kopiert. Derselbe Agent verwendet seine Datei bei späteren Steps des Runs weiter; neue Runs erhalten neue Dateien.
- Run-Dateien werden nicht versioniert. Keine Geheimnisse in ihnen ablegen.

Die CLI führt nach dem Erstellen des Snapshots ausschließlich die daraus geladene Konfiguration aus. Agentenschritte starten Pi als separaten Prozess; Pi muss installiert und für die in den Agenten-TOML-Dateien konfigurierten Modelle eingerichtet sein. Start:

```bash
cargo run -p factory-cli -- feature "Beschreibe hier die gewünschte Änderung."
```

Shell-Steps starten echte Prozesse. Nur vertrauenswürdige Pipelines ausführen.
