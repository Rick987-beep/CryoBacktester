"""
state.py — Reactive application state using Panel/param.

AppState is a param.Parameterized class so any view can watch any field
with state.param.watch(cb, ["active_run_id"]).

URL sync (pn.state.location) is wired in app.py after the template is built.
"""
import param


class AppState(param.Parameterized):
    """Central reactive state shared across all views."""

    active_run_id = param.Integer(default=None, allow_None=True,
                                  doc="Currently loaded run id (None = nothing loaded)")

    selected_combo_keys = param.List(default=[],
                                     doc="List of param-tuple keys currently selected in the grid")

    active_tab = param.String(default="Results Grid",
                              doc="Name of the currently visible main-pane tab")

    # Phase 2: the combo key the user is inspecting in Combo Detail
    active_combo_key = param.Parameter(default=None, allow_None=True,
                                       doc="Param-tuple key of the combo shown in the Detail tab")

    # URL-serialisable hash of active_combo_key (12-char hex); kept in sync automatically.
    active_combo_hash = param.String(default="",
                                     doc="key_hash of active_combo_key — synced to URL ?combo=")

    # Phase 3: active run handle (None when no run in progress)
    active_run_handle = param.Parameter(default=None, allow_None=True,
                                        doc="RunHandle while a backtest is running; None otherwise")

    # Phase 4: signal from Favourites "Re-run" button
    # When set to {"strategy": str, "param_grid": dict}, the sidebar picks it up
    # and prefills its controls, then resets this to None.
    rerun_request = param.Parameter(default=None, allow_None=True,
                                    doc="Dict {strategy, param_grid} to prefill the sidebar")
