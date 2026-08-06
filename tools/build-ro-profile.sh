#!/usr/bin/env bash
# Build the read-only agy profile used by the proxy's `ro` role.
#
# The proxy spawns agy with HOME=$AGY_RO_HOME for any model id ending in `-ro`.
# That HOME holds an agy config whose settings.json has NO
# --dangerously-skip-permissions and a narrow permissions.allow of read-only
# tools. Verified on agy 1.1.10 (see PLAN_ACL_LIMITS.md §7):
#   - a tool NOT on the allowlist is auto-DENIED in --print mode (clean, no hang)
#   - permission matching is EXACT tool name — no substring, no glob, so every
#     read tool must be enumerated explicitly below
#   - a HOME with a SYMLINKED oauth token works with no re-login
#
# Idempotent: rebuilds $DEST from scratch each run. Re-run after any change to
# the source MCP config or the allowlist. Does not touch the production (full)
# profile in ~/.gemini.
set -euo pipefail

SRC="${AGY_SRC_HOME:-$HOME}/.gemini"
DEST="${AGY_RO_HOME:-$HOME/.agy-profiles/ro}/.gemini"
WORKSPACE="${AGY_RO_WORKSPACE:-$HOME/projects/homeassistant}"

# Read-only allowlist. Names verified against the live ha-mcp server. Only
# get_/list_/search/config_get_/config_list_ tools — every write/manage/remove/
# call_service/bulk_control tool is deliberately absent, so it is auto-denied.
RO_TOOLS=(
  ha_get_state ha_get_entity ha_get_device ha_get_history ha_get_overview
  ha_search ha_list_services ha_list_floors_areas ha_get_integration
  ha_get_addon ha_get_hacs_info ha_get_blueprint ha_get_camera_image
  ha_get_operation_status ha_eval_template
  ha_config_get_automation ha_config_get_script ha_config_get_scene
  ha_config_get_dashboard ha_config_get_category ha_config_get_label
  ha_config_get_calendar_events ha_config_list_helpers ha_config_list_groups
  ha_config_list_dashboard_resources
)
# Read-only shell commands (agy also gates commands via permissions.allow).
RO_COMMANDS=(ls cat grep head find which echo)

# MCP servers to KEEP in the RO profile. nextcloud-rag is dropped: heavy,
# long-lived process that is itself a source of false watchdog signals, and not
# needed for a read-only HA assistant.
KEEP_MCP='["ha-mcp","agy-history","gemini-web"]'

echo "Building RO profile at: $DEST"
rm -rf "$DEST"
mkdir -p "$DEST/antigravity-cli" "$DEST/config"

# --- token: symlink to the real one (refresh writes back to a single place) ---
tok="$SRC/antigravity-cli/antigravity-oauth-token"
if [ ! -e "$tok" ]; then echo "ERROR: no oauth token at $tok — log in with full agy first" >&2; exit 1; fi
ln -s "$tok" "$DEST/antigravity-cli/antigravity-oauth-token"

# --- copied identity + MCP configs (nextcloud-rag stripped) ---
cp "$SRC/antigravity-cli/installation_id" "$DEST/antigravity-cli/" 2>/dev/null || true
[ -f "$SRC/config/config.json" ] && cp "$SRC/config/config.json" "$DEST/config/"

strip_mcp() {  # $1 = source mcp_config.json, $2 = dest
  [ -f "$1" ] || return 0
  KEEP="$KEEP_MCP" python3 - "$1" "$2" <<'PY'
import json, os, sys
src, dst = sys.argv[1], sys.argv[2]
keep = set(json.loads(os.environ["KEEP"]))
d = json.load(open(src))
servers = d.get("mcpServers", {})
d["mcpServers"] = {k: v for k, v in servers.items() if k in keep}
json.dump(d, open(dst, "w"), indent=2)
print(f"  {os.path.basename(os.path.dirname(dst))}/mcp_config.json -> {sorted(d['mcpServers'])}")
PY
}
strip_mcp "$SRC/antigravity-cli/mcp_config.json" "$DEST/antigravity-cli/mcp_config.json"
strip_mcp "$SRC/config/mcp_config.json"          "$DEST/config/mcp_config.json"

# --- settings.json: the heart of the ACL ---
python3 - "$DEST/antigravity-cli/settings.json" "$WORKSPACE" <<PY
import json, sys
allow  = [f"mcp(ha-mcp/{t})" for t in "${RO_TOOLS[*]}".split()]
allow += [f"command({c})"    for c in "${RO_COMMANDS[*]}".split()]
allow += ["mcp(agy-history/agy_history_search)", "mcp(agy-history/agy_history_get)",
          "mcp(agy-history/agy_history_grep)",
          "mcp(gemini-web/gemini_web_search)", "mcp(gemini-web/gemini_web_get)",
          "mcp(gemini-web/gemini_web_status)"]
cfg = {
  "allowNonWorkspaceAccess": False,
  "trustedWorkspaces": [sys.argv[2]],
  "enableTelemetry": False,
  "permissions": {"allow": allow},
}
json.dump(cfg, open(sys.argv[1], "w"), indent=2)
print(f"  settings.json -> {len(allow)} allow entries (read-only)")
PY

echo "Done. Verify with:  env HOME=${AGY_RO_HOME:-$HOME/.agy-profiles/ro} agy --print 'stan sun.sun'"
echo "A write (e.g. ha_call_service) MUST be auto-denied."
