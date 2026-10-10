"""Package-owned forms, sharing only the framework's public rendering helpers."""

from html import escape
import json
from bloxsmith_app.block_api import render_inspector_template, render_node_card_template


def replace_config(result, node):
    """Replace edited settings exactly; generic graph patches would otherwise deep-merge objects."""
    patch = result.get("node_patch") if isinstance(result, dict) else None
    if not isinstance(patch, dict) or not isinstance(patch.get("config"), dict):
        return result
    settings = {**(node.get("config") or {}), **patch.pop("config")}
    result.setdefault("graph_operations", []).append({"op": "update_node_config",
        "node_id": str(node.get("id") or ""), "config": settings, "replace": True})
    return result


def fields(block, node, surface, between=""):
    """Render trusted field definitions with escaped values; config keys stay untranslated labels.

    ``between`` is trusted block-owned HTML placed after the main fields, before the advanced ones.
    """
    specs = json.loads((block.directory / "fields.json").read_text(encoding="utf-8"))
    config = {**block.default_config(), **(node.get("config") or {})}
    groups = {"main": [], "advanced": []}
    prefix = escape(f"{block.kind}-{node.get('id', '')}-{surface}", quote=True)
    for spec in specs:
        key, kind = spec["key"], spec.get("type", "text")
        value = config.get(key, "")
        ident = f"{prefix}-{key}"
        attrs = f'data-block-config-field="{key}"'
        if kind in {"integer", "number", "boolean"}:
            attrs += f' data-block-value-type="{kind}"'
        hint_key = f"block.{block.kind}.help_{key}"
        hint = escape(block.translate(hint_key, fallback=spec["help"]))
        help_html = f'<p id="{ident}-hint" class="field-hint" data-i18n="{hint_key}">{hint}</p>'
        common = f'id="{ident}" {attrs} aria-describedby="{ident}-hint"'
        if kind == "select":
            options = "".join(f'<option value="{escape(option, quote=True)}"{" selected" if option == value else ""}>{escape(option)}</option>'
                              for option in spec["options"])
            control = f'<select {common}>{options}</select>'
        elif kind == "boolean":
            control = f'<input {common} type="checkbox"{" checked" if value else ""}>'
        elif kind in {"integer", "number"}:
            step = "1" if kind == "integer" else "any"
            control = (f'<input {common} type="number" inputmode="decimal" min="{spec["min"]}" max="{spec["max"]}" '
                       f'step="{step}" value="{escape(str(value), quote=True)}">')
        else:
            placeholder = escape(spec.get("placeholder", ""), quote=True)
            control = (f'<input {common} type="text" autocomplete="off" spellcheck="false" '
                       f'placeholder="{placeholder}" value="{escape(str(value), quote=True)}">')
        rendered = f'<div class="field-group"><label for="{ident}">{escape(key)}</label>{control}{help_html}</div>'
        if spec.get("modes"):
            hidden = "" if config.get("mode", "duration") in spec["modes"].split(",") else " hidden"
            rendered = f'<div class="owned-mode-field" data-wait-modes="{escape(spec["modes"], quote=True)}"{hidden}>{rendered}</div>'
        groups["advanced" if spec.get("advanced") else "main"].append(rendered)
    html = '<div class="owned-fields">' + "".join(groups["main"]) + "</div>" + between
    if groups["advanced"]:
        key = f"block.{block.kind}.advanced"
        html += (f'<details class="owned-advanced"><summary data-i18n="{key}">'
                 f'{escape(block.translate(key, fallback="Advanced settings"))}</summary>'
                 f'<div class="owned-fields">{"".join(groups["advanced"])}</div></details>')
    return html


def usage(block):
    """Static, translated guidance about commands and the One Shot limit."""
    commands = escape(block.translate("block.wait.usage_commands",
        fallback='On command_in, {"action":"flush"} releases every pending message now and {"action":"cancel"} drops them.'))
    one_shot = escape(block.translate("block.wait.usage_one_shot",
        fallback="Active Runtime keeps messages waiting without blocking the graph. One Shot waits inside the activation, at most 240 s."))
    return (f'<div class="owned-usage"><p class="field-hint" data-i18n="block.wait.usage_commands">{commands}</p>'
            f'<p class="field-hint" data-i18n="block.wait.usage_one_shot">{one_shot}</p></div>')


def modal(block, node, payload):
    """Use the generic draft host; runtime refresh must not replace pending settings."""
    template = (block.directory / "block_modal.html").read_text(encoding="utf-8")
    html = block._render_generic_modal_template(template=template, node=node, payload=payload or {})
    html = html.replace("{{ settings }}", fields(block, node, "modal", usage(block)))
    return {"html": html, "context": {"node_id": str(node.get("id", "")), "node_kind": block.kind}}


def inspector(block, node, payload):
    """Render the same settings with the standard Ports tab and a sticky Apply control."""
    template = (block.directory / "inspector_panel.html").read_text(encoding="utf-8")
    html = render_inspector_template(template=template, node={**node, "kind": block.kind, "type": block.kind},
                                    payload=payload, replacements={"settings": fields(block, node, "inspector", usage(block))})
    return {"html": html, "context": {"node_id": str(node.get("id", "")), "full_panel": True}}


def preview(block, config):
    """One short translated line describing the rule; raw values are not markers."""
    if config.get("mode") == "until":
        if config.get("until_path"):
            return block.translate("block.wait.card_until_path", {"path": config["until_path"]},
                                   fallback="Until the instant at {path}")
        return block.translate("block.wait.card_until", {"instant": config.get("until") or "?"},
                               fallback="Until {instant}")
    return block.translate("block.wait.card_duration", {"seconds": config.get("seconds")}, fallback="Delay {seconds} s")


def card(block, node):
    """Keep name, purpose and current rule inside the existing card hierarchy."""
    config = {**block.default_config(), **(node.get("config") or {})}
    return render_node_card_template(block=block, node=node, node_classes=[block.kind + "-node"],
           replacements={"title": node.get("title") or block.default_title(), "preview": preview(block, config),
                         "purpose": block.description()})
