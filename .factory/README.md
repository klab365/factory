# Pipeline-Konfiguration

- `agents/` enthält wiederverwendbare Agentendefinitionen.
- `pipelines/feature.toml` beschreibt Issue-Intake, Planung sowie Build-/Review-/Test-Ablauf.
- Jeder Pipeline-Run erzeugt unter `runs/<run-id>/` einen atomar gespeicherten `run.json`-Journal, einen `config.json`-Snapshot und genau eine Markdown-Datei pro eindeutigem Agenten der Pipeline, z. B. `00-coder.md` und `01-reviewer.md`.
- Der Auftrag wird in jede Agenten-Datei kopiert. Derselbe Agent verwendet seine Datei bei späteren Steps des Runs weiter; neue Runs erhalten neue Dateien.
- `run.json` protokolliert Status, aktuellen Step, Step-Versuche, Fehler, Outcomes und Handoffs. Es wird vor/nach jedem Step atomar aktualisiert.
- Andere Agenten-MDs bleiben privat. Der Orchestrator übergibt strukturierte Reviewer-Findings in die Coder-MD; der Coder muss keine Reviewer-Datei lesen.
- Run-Dateien werden nicht versioniert. Keine Geheimnisse in ihnen ablegen.

Die CLI führt nach dem Erstellen des Snapshots ausschließlich die daraus geladene Konfiguration aus. Agentenschritte starten Pi als separaten Prozess; Pi muss installiert und für die in den Agenten-TOML-Dateien konfigurierten Modelle eingerichtet sein. Start:

```bash
cargo run -p factory-cli --bin factory -- feature --input issue_url=https://github.com/owner/repo/issues/123 "Löse das GitHub-Issue"
# Status und Step-Fehler eines Runs inspizieren:
cargo run -p factory-cli --bin factory -- run show <run-id>
# Nach einem Abbruch an der nächsten Step-Grenze fortsetzen:
cargo run -p factory-cli --bin factory -- run resume <run-id>
# Wenn ein Step selbst unvollständig war, muss seine Wiederholung bestätigt werden:
cargo run -p factory-cli --bin factory -- run resume <run-id> --retry-current
```

Pipeline-Eingaben werden als `[[inputs]]` mit `key` und `description` deklariert. Ein Start ohne Task, z. B. `factory feature` oder `cargo run -p factory-cli --bin factory -- feature`, fragt die Pipeline-Eingaben und danach den Task interaktiv ab. Fehlende Eingaben können beim Start auch als `--input name=value` übergeben werden. In Shell-Argumenten werden sie mit `{{name}}` eingesetzt; die Ersetzung erfolgt als separates Prozessargument. Der Issue-Intake in `feature.toml` nutzt diese generische Funktion für `issue_url`. Die Issue-Daten werden als Step-Ergebnis gespeichert und dem Planner-Agenten als Handoff sowie in dessen Run-Markdown übergeben. `gh` muss installiert und authentifiziert sein.

Shell-Steps starten echte Prozesse. Nur vertrauenswürdige Pipelines ausführen.
