1a — Concurrency / Pairing
Ja, Ihre Interpretation ist korrekt. Technisch wird jedes Entry-Ereignis dem chronologisch nächsten Exit-Ereignis nach diesem Entry zugeordnet (pair_first_entry_then_next_exit). Dabei wird der Exit-Index nicht nach jeder Zuordnung erhöht — daher können mehrere Entries vor demselben Exit-Bar liegen und denselben gemeinsamen Exit erhalten. Das erklärt das systematische Muster in den Referenz-Signaldateien („Fan-out“ zum gleichen Exit). Das ist kein striktes Mutex-„ein Slot nur“ aus der früheren Pseudocode-Skizze.
1b — Maxim gleichzeitiger Positionen
In dieser Paarungs- und Simulationslogik gibt es keine Obergrenze für gleichzeitige Positionen aus der Regel heraus — die Anzahl gemeinsamer Exits ergibt sich nur aus der Häufigkeit der Entry-Signale vor dem nächsten Exit. Ein bewusst gesetztes Cap (z. B. maximal 3 offene Kontrakte) wäre eine zusätzliche Live-/Risiko-Policy, die nicht in der Referenz-Konstruktion der Signale liegt.
1c — Bull-Sleeve (EMA)
Der Bull-Arm verwendet dieselbe paarweise Struktur (bullisches Kreuz als „Entry“, bearisches Kreuz als „Exit“, jeweils nächste passende Kombination nach Zeit). Fan-out ist bei EMA typischerweise seltener als bei RSI, aber die Regel ist dieselbe: mehrere Entries vor einem Exit können denselben Exit-Bar teilen, sobald die Indikatorreihenlage das zulässt.
2a — Ablauf, wenn die Option vor dem Trigger-Exit verfällt
Die Simulation arbeitet pro Signalzeile separat, ohne einen globalen „Pending-Slot“. Der ökonomische Ausstieg liegt bei min/zuerst: Optionsverfall (Intrinsik-Settlement) gegenüber Ausstieg zum Signal-Exit-Zeitpunkt, so wie es Ihre beiden Bull-Beispiele mit exit_kind = expiry zeigen: fill_exit_utc vor signal_exit_utc, der Signal-Exit bleibt Indikator-Label.
Damit gilt: Das System „wartet“ intern nicht, bis der spätere RSI/EMA-Signal-Bar erreicht ist, um andere Logik zu entblocken — die Signaltabelle wird aus der ganzen Zeitreihe gebaut. Für einen Live-Bot sollten Sie die Positionslogik am effektiven Schließzeitpunkt ausrichten, nicht allein am signal_exit_utc, wenn der frühere Verfall dominiert.
2b — Gültigkeit Bull & Bear
Ja, bestätigt: dieselbe Kombination aus rechtzeitigem Verfall intrinsisch versus Ausstieg per Quote am Signal-Bar gilt gleichermaßen für Calls (Bull) und Puts (Bear) in der Referenzsimulation (instrument „call“/„put“ mit passender Intrinsik-Formel). Bei längeren Bear-DTEs ist Verfall vor Trigger weniger häufig, die Regel aber identisch.
3a — Delta-Toleranz
Es ist kein festes Fenster (z. B. ±0,05) definiert. Vorgehensweise im Backtest: nächstliegendes Delta zum Ziel, mit Strike-Tiebreak bei gleichem Abstand (Calls u. a. kleinerer Strike bei Gleichstand; Puts sortiert entsprechend der Implementierung).
3b — Kein Kontrakt im „Toleranz“-Sinn
Es gibt keinen zweiten Pfad wie „inside band only“. Entweder es gibt einen handelbaren nächstliegenden Kontrakt in der Kette — oder das Paar wird übersprungen (z. B. fehlende Quotes / keine passende Delta-Zeile).
Wir empfehlen, die frühere vereinfachte Pseudocode-Skizze mit Mutex durch die oben beschriebene pair_first_entry_then_next_exit-Semantik zu ersetzen, damit Implementierungen 1:1 mit den ausgelieferten Signal- und Trade-Dateien übereinstimmen.

