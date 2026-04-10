import os
import re

templates_dir = r"c:\Users\Dell\Desktop\Segmento-app-website-dev\Segmento-collector\frontend\templates\connectors"

FIXED_MARKER = "/* UI-DISCONNECT-FIX-v1 */"

# ── Fix 1: updateFlowUI ───────────────────────────────────────────────────────
# When step >= 1 (has credentials, not connected) → show form, hide successState
# Only the `step >= 3` branch currently does that swap; step 1 leaves successState visible.

OLD_flowUI = """    if (step >= 1) {"""

NEW_flowUI = """    // PATCH: ensure successState is hidden when not fully connected
    if (success) { success.classList.add("hidden"); success.classList.remove("opacity-100"); }
    if (form) { form.classList.remove("opacity-0", "hidden"); }

    if (step >= 1) {"""

# ── Fix 2: disconnect function – replace reload with status re-fetch ──────────
# Pattern: after res.ok, the template calls location.reload()
# We replace it with a clean DOM update (no reload needed because checkStatus
# will hide successState and show the form).

OLD_reconnect = """        await fetch("/connectors/"""
# (This isn't the right match – too broad. We'll match the specific pattern.)

# Better: find `location.reload()` inside any disconnect function and replace
OLD_reload_block = """      if (res.ok) {
        alert("Connection revoked.");
        location.reload();
      }"""

NEW_reload_block = """      if (res.ok) {
        // Re-fetch status so UI swaps without full reload
        const statusFnName = Object.keys(window).find(k =>
          k.startsWith("check") && k.toLowerCase().includes("status") && typeof window[k] === "function"
        );
        if (statusFnName) {
          await window[statusFnName]();
        } else {
          location.reload();
        }
      }"""

# ── Fix 3: Handle the case where successState needs to be hidden
#    in the `else` block of checkStatus (not connected, no credentials).
#    We inject `if (success) { success.classList.add("hidden"); }` there.
#    But this is already handled by Fix 1 (updateFlowUI) since step 1 now hides it.

fixed_count = 0
skipped_count = 0
errors = []

for fname in os.listdir(templates_dir):
    if not fname.endswith(".html"):
        continue
    fpath = os.path.join(templates_dir, fname)
    with open(fpath, "r", encoding="utf-8") as f:
        content = f.read()

    if FIXED_MARKER in content:
        skipped_count += 1
        continue

    original = content

    # Apply Fix 1: patch updateFlowUI
    if OLD_flowUI in content:
        content = content.replace(OLD_flowUI, NEW_flowUI, 1)

    # Apply Fix 2: replace location.reload() in disconnect handlers
    if OLD_reload_block in content:
        content = content.replace(OLD_reload_block, NEW_reload_block)

    # Mark as patched
    content = content.replace("<script>", f"<script>\n  {FIXED_MARKER}", 1)

    if content != original:
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)
        fixed_count += 1
        print(f"  [FIXED] {fname}")
    else:
        skipped_count += 1

print(f"\nDone. Fixed: {fixed_count}, Skipped (no match or already done): {skipped_count}")
if errors:
    print("Errors:", errors)
