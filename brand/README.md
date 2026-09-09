# Brand assets (cryo_aureas)

Emitted / mirrored from the agent-commons pack. **Do not invent hex** — rebuild
from the pack via `ac-brand emit`.

## Runtime vs local specimen

| Path | Role |
|------|------|
| `backtester/ui/brand/` | **Shipped** into the Panel Research UI (`research_light.css`, icon, tokens) |
| `brand/` (this folder) | Local specimen, mockups, and full pack emit for design review |

```bash
# Refresh pack copies into this folder (maintainer machine with agent-commons)
ac-brand emit -o ./brand --surface all --with-icons
# Then copy the light research surface into the product tree if tokens changed:
#   brand/research_light.css → backtester/ui/brand/research_light.css
#   brand/icons/backtester.png → backtester/ui/brand/icons/backtester.png
```

## Files here

- `tokens.json`, `manifest.json`
- `document.css`, `ops.css`, `ops_theme.json`
- `research_light.css` — light Research UI shell (same family as product copy)
- `icons/` — product family icons
- `specimen.html` — static chrome / control specimen (`python -m http.server` from here)
- `mockups/backtester_run.html` — layout mock for the Backtester Run page

Visual law: `~/agent-commons/docs/cryo-aureas/DESIGN_DECISION.md`  
Skill: `brand` (agent-commons)
