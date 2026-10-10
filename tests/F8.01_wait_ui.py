"""FB8: release UI in Chromium: mode-specific fields, drafts, validation, persistence, EN/FR, responsive."""

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from playwright.sync_api import sync_playwright, expect
from block_test_artifacts import artifact_path
from block_test_packages import install_test_package
from ui_smoke_common import isolated_server, graph_payload, create_project_api, project_editor_url, http_json
from blocs.wait.block import WaitBlock


def geometry(modal):
    """Require an opaque panel with reachable actions, labelled controls and no horizontal overflow."""
    measured = modal.evaluate("""el => {
        const r=el.getBoundingClientRect(), body=el.querySelector('.owned-body');
        const actions=[...el.querySelectorAll('[data-close-block-modal], [data-owned-apply]')];
        return {inside:r.left>=-1&&r.right<=innerWidth+1&&r.top>=-1&&r.bottom<=innerHeight+1,
          overflow:el.scrollWidth>el.clientWidth+2||body.scrollWidth>body.clientWidth+2,
          controls:actions.every(a=>{const b=a.getBoundingClientRect();return b.top>=0&&b.bottom<=innerHeight+1}),
          background:getComputedStyle(el).backgroundColor,
          unlabeled:[...el.querySelectorAll('input,select,textarea')].filter(c=>!c.labels?.length&&!c.getAttribute('aria-label')&&!c.getAttribute('aria-labelledby')).length};
    }""")
    assert measured["inside"] and measured["controls"] and not measured["overflow"], measured
    assert not measured["unlabeled"], measured
    assert measured["background"] not in {"transparent", "rgba(0, 0, 0, 0)"}, measured


def saved_config(server, project):
    """Read the persisted node configuration through the public project API."""
    graph = http_json(server.base_url, f"/api/projects/{project['project_id']}/graph")
    document = graph.get("document") or graph.get("graph") or graph
    return next(node for node in document["nodes"] if node["id"] == "ui-case")["config"]


def main():
    """Exercise current package files in real managed and linked browser hosts."""
    # - FB8 - translated responsive settings with mode-specific fields and draft/Apply.
    for origin in ("managed", "linked"):
        with isolated_server() as server, sync_playwright() as playwright:
            model = install_test_package(server, "wait", origin=origin)
            candidate = WaitBlock().build_node_payload(node_id="ui-case", position={"x": 160, "y": 160})
            candidate["block_version"] = model["version"]
            project = create_project_api(server, document=graph_payload("Owned form", [candidate], []))["project"]
            url = project_editor_url(server.base_url, project["project_id"], workspace_project_id=project["workspace_project_id"])
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1440, "height": 900})
                page.add_init_script("window.localStorage.setItem('bloxsmith.inspectorPinned','true')")
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto(url)
                card = page.locator('.canvas-node[data-node-id="ui-case"]')
                expect(card.locator('h3')).to_be_visible()
                expect(card.locator('.node-data-preview')).to_have_text("Delay 5 s")
                card.locator('h3').dblclick()
                modal = page.locator('.owned-modal')
                expect(modal).to_be_visible()
                apply = modal.locator('[data-owned-apply]')
                expect(apply).to_be_disabled()
                seconds = modal.locator('[data-block-config-field="seconds"]')
                mode = modal.locator('[data-block-config-field="mode"]')
                until = modal.locator('[data-block-config-field="until"]')
                path = modal.locator('[data-block-config-field="until_path"]')
                expect(seconds).to_be_visible()
                expect(until).not_to_be_visible()
                mode.select_option("until")
                expect(until).to_be_visible()
                expect(path).to_be_visible()
                expect(seconds).not_to_be_visible()
                expect(apply).to_be_enabled()
                # until mode without an instant or a path is refused by the block, not saved.
                with page.expect_response(lambda r: r.url.endswith('/ui-action') and r.request.method == 'POST') as refused:
                    apply.click()
                assert "until" in refused.value.json().get("error", ""), refused.value.json()
                expect(modal.locator('.owned-form-status')).to_contain_text("until")
                assert saved_config(server, project)["mode"] == "duration"
                # Closing discards the draft.
                modal.locator('[data-close-block-modal]').last.click()
                card.locator('h3').dblclick()
                expect(mode).to_have_value("duration")
                expect(seconds).to_have_value("5")
                seconds.fill("12.5")
                expect(apply).to_be_enabled()
                with page.expect_response(lambda r: r.url.endswith('/ui-action') and r.request.method == 'POST') as saved:
                    apply.click()
                assert not saved.value.json().get("error"), saved.value.json()
                expect(modal.locator('.owned-form-status')).to_have_text("Settings applied.")
                assert saved_config(server, project)["seconds"] == 12.5
                mode.select_option("until")
                path.fill("/release_at")
                with page.expect_response(lambda r: r.url.endswith('/ui-action') and r.request.method == 'POST') as saved:
                    apply.click()
                assert not saved.value.json().get("error"), saved.value.json()
                stored = saved_config(server, project)
                assert stored["mode"] == "until" and stored["until_path"] == "/release_at" and stored["seconds"] == 12.5
                if modal.is_visible():
                    modal.locator('[data-close-block-modal]').first.click()
                page.reload()
                expect(card.locator('.node-data-preview')).to_have_text("Until the instant at /release_at")
                card.locator('h3').dblclick()
                expect(path).to_have_value("/release_at")
                expect(seconds).not_to_be_visible()
                for width, height in ((1440, 900), (800, 700), (390, 740), (320, 568)):
                    page.set_viewport_size({"width": width, "height": height})
                    page.wait_for_timeout(100)
                    geometry(modal)
                    page.screenshot(path=artifact_path(f"wait-{origin}-{width}.png"))
                summary = modal.locator('.owned-advanced').first.locator('summary')
                summary.scroll_into_view_if_needed()
                summary.focus()
                summary.press('Enter')
                expect(modal.locator('.owned-advanced').first).to_have_attribute('open', '')
                expect(modal.locator('[data-block-config-field="max_pending"]')).to_be_visible()
                geometry(modal)
                page.screenshot(path=artifact_path(f"wait-{origin}-advanced.png"))
                page.set_viewport_size({"width": 1440, "height": 900})
                modal.locator('[data-close-block-modal]').first.click()
                card.click(position={"x": 20, "y": 20})
                inspector = page.locator('[data-properties-surface="inspector"][data-node-id="ui-case"]:visible')
                expect(inspector).to_be_visible()
                assert inspector.evaluate('el => el.scrollWidth <= el.clientWidth+2')
                expect(inspector.locator('[data-block-config-field="until_path"]')).to_have_value("/release_at")
                inspector_mode = inspector.locator('[data-block-config-field="mode"]')
                inspector_mode.select_option("duration")
                expect(inspector.locator('[data-block-config-field="seconds"]')).to_be_visible()
                expect(inspector.locator('[data-block-config-field="until_path"]')).not_to_be_visible()
                inspector.locator('[data-block-config-field="seconds"]').fill("7")
                inspector_apply = inspector.locator('[data-owned-apply]')
                expect(inspector_apply).to_be_enabled()
                with page.expect_response(lambda r: r.url.endswith('/ui-action') and r.request.method == 'POST') as saved:
                    inspector_apply.click()
                assert not saved.value.json().get("error"), saved.value.json()
                stored = saved_config(server, project)
                assert stored["mode"] == "duration" and stored["seconds"] == 7 and stored["until_path"] == "/release_at"
                expect(card.locator('.node-data-preview')).to_have_text("Delay 7 s")
                page.screenshot(path=artifact_path(f"wait-{origin}-inspector.png"))
                # Tab changes re-render the inspector from the saved node (shared framework behavior).
                inspector.locator('[data-inspector-tab="ports"]').click()
                expect(inspector.locator('[data-inspector-panel-tab="ports"]')).to_be_visible()
                inspector.locator('[data-inspector-tab="general"]').click()
                expect(inspector.locator('[data-block-config-field="seconds"]')).to_have_value("7")
                page.goto(server.base_url + "/")
                page.locator("#homeApplicationSettingsButton").click()
                page.locator("#applicationLanguageSelect").select_option("fr")
                page.wait_for_function("window.CWMessages.getLanguage() === 'fr'")
                page.goto(url)
                card.locator('h3').dblclick()
                expect(modal.locator('.owned-advanced').first.locator('summary')).to_have_text("Réglages avancés")
                expect(modal.locator('[data-i18n="block.wait.help_until_path"]')).to_contain_text("JSON Pointer facultatif")
                expect(modal.locator('.panel-kicker')).to_have_text("Contrôle du flux")
                geometry(modal)
                page.screenshot(path=artifact_path(f"wait-{origin}-fr.png"))
                assert not errors, errors
                assert page.evaluate("!window.CWBlockUiBlocks?.wait")
            finally:
                browser.close()
        print("[ok] wait " + origin + " UI", flush=True)


if __name__ == "__main__":
    main()
