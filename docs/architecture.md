# Architektur-Notizen

## Zielbild

Die Software Factory definiert Pipelines und Agenten in Projektkonfigurationen und führt sie über austauschbare Harnesses aus. Der Workflow-Core soll unabhängig davon bleiben, ob der Start über CLI, Pi oder später eine HTTP/Web-API erfolgt.

```text
Pi / CLI / HTTP
       |
factory-core::features
       |
factory-core <-- Ports <-- Harness-, Shell-, Git- und Persistence-Adapter
```

- **Core** enthält Domänenmodelle und Ablaufregeln. Die Use-Cases liegen als Module direkt unter `factory-core::features`; auch diese Module haben keine TOML-, Pi-, CLI- oder HTTP-Abhängigkeiten.
- **Application** orchestriert Anwendungsfälle wie Pipeline starten, pausieren und fortsetzen und nutzt Runner-Ports.
- **Adapter** implementieren die Außenwelt: TOML laden, Agenten/Harnesses starten, Shell-Befehle ausführen, Git bedienen und Runs speichern.
- Pi ist zunächst ein Einstiegspunkt bzw. eine UI, nicht Teil der Workflow-Domäne. Später können CLI und Webserver dieselben Application-Use-Cases aufrufen.

## Konfigurationsformat und Struktur

TOML ist das zunächst gewählte Format. Agenten und Pipelines werden getrennt abgelegt, damit Agentendefinitionen wiederverwendet werden können:

```text
.factory/
  agents/
    planner.toml
    coder.toml
    reviewer.toml
  pipelines/
    feature.toml
    bugfix.toml
```

Eine Agentendefinition enthält mindestens `name`, `harness`, `model`, `thinking_level` und `prompt`. Das Modell hat die Form `provider/model`, beispielsweise `anthropic/claude-sonnet-4`. `harness` beschreibt den ausführenden Adapter; es ist unabhängig vom Provider des Modells. Der Prompt liegt zunächst inline als mehrzeiliger TOML-String.

Pipelines definieren Schritte mit explizitem `type = "agent"` oder `type = "shell"`. Agentenschritte referenzieren Agenten per Namen. Übergänge bilden Entscheidungen und Schleifen ab, zum Beispiel Review-Ergebnis `changes_requested` zurück zu `build` oder `approved` weiter zu `commit`.

Unter `.factory/runs/<run-id>/` liegt das run-spezifische Agentengedächtnis: Jeder Agent hält dort im Rahmen seines Runs Kontext, getroffene Entscheidungen, Ergebnisse und offene Punkte fest. Diese Dateien dokumentieren den Verlauf eines einzelnen Runs und ergänzen, statt die projektweite Agenten- und Pipeline-Konfiguration zu ersetzen.

## Ablauf und Ergebnisse

Der Core sollte keine frei formulierten Agentenantworten selbst als Kontrollfluss interpretieren. Adapter bzw. Application-Schicht liefern strukturierte Ergebnisse und benannte Outcomes; die Pipeline definiert, welches Outcome zu welchem Schritt führt. Schleifen und Wiederholungen müssen begrenzt sein, damit fehlerhafte Konfigurationen keine Endlosschleifen erzeugen.

Shell-Schritte brauchen mindestens ein konfigurierbares Arbeitsverzeichnis, Timeout, Abbruchverhalten und kontrollierte Umgebungsvariablen. Da sie beliebige Prozesse starten können, sollten sie als Sicherheitsgrenze behandelt werden; insbesondere dürfen nicht vertrauenswürdige Pipeline-Dateien nicht automatisch ausgeführt werden.

## `factory-core`

`packages/factory-core` enthält die formatneutralen Domänentypen für Agenten, Modellreferenzen, Thinking-Level sowie Pipelines, Schritte und Übergänge. Die Use-Cases liegen als Module direkt unter `factory-core::features` und orchestrieren diese Modelle über Ports. TOML-Deserialisierung gehört in `factory-adapters::config`; Harness-spezifische Umsetzung und Prozessausführung gehören ebenfalls in Adapter.

Die Thinking-Level sind zunächst als gemeinsames Set modelliert (`off`, `minimal`, `low`, `medium`, `high`, `xhigh`). Adapter müssen prüfen bzw. übersetzen, welche Werte ein konkreter Harness oder ein Modell unterstützt.

## Entwicklung und Pi

`mise.toml` verwaltet lokale Development-Tools und Tasks, nicht die Laufzeitkonfiguration unter `.factory/`. Der Task `dev-pi` soll Pi im aktuellen Projektverzeichnis starten, damit die Entwicklung direkt im Repo getestet werden kann. Das Starten von Pi allein integriert noch keine Factory-Pipeline; dafür wird später eine Pi-Extension oder ein Aufruf der Application-Use-Cases benötigt.

## Sinnvolle nächste Schritte

1. TOML-Modelle und Validierung in einem Config-Adapter ergänzen; Agentenreferenzen und doppelte Step-IDs beim Laden prüfen.
2. Workflow-Ausführung im Application-Layer auf Basis von `factory-core` implementieren.
3. Einen Agenten- und Shell-Runner als Adapter hinzufügen und mit kleinen Integrationstests absichern.
4. CLI zum Starten und Anzeigen eines Pipeline-Runs ergänzen; Pi- und HTTP-Integrationen können danach denselben Application-Port nutzen.
