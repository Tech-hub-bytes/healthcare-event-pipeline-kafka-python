from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def _decode_xml_entities(s: str) -> str:
    s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&apos;", "'")
    s = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), s)
    s = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), s)
    return s.replace("&amp;", "&")


def strip_tags(html: str) -> str:
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    html = re.sub(r"</p>", "\n", html, flags=re.I)
    html = re.sub(r"</tr>", "\n", html, flags=re.I)
    html = re.sub(r"</th>", " | ", html, flags=re.I)
    html = re.sub(r"</td>", " | ", html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n[ \t]+", "\n", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return _decode_xml_entities(html).strip()


def extract_elements(xml: str, local_name: str) -> list[str]:
    open_re = re.compile(rf"<(?:[\w.-]+:)?{local_name}\b[^>]*>", re.I)
    close_re = re.compile(rf"</(?:[\w.-]+:)?{local_name}\s*>", re.I)
    results: list[str] = []
    pos = 0
    while True:
        m = open_re.search(xml, pos)
        if not m:
            break
        start = m.start()
        after_open = m.end()
        if m.group(0).endswith("/>"):
            results.append(m.group(0))
            pos = after_open
            continue
        depth = 1
        search_from = after_open
        end = -1
        while depth > 0:
            next_open = open_re.search(xml, search_from)
            next_close = close_re.search(xml, search_from)
            if not next_close:
                break
            if next_open and next_open.start() < next_close.start():
                if not next_open.group(0).endswith("/>"):
                    depth += 1
                search_from = next_open.end()
            else:
                depth -= 1
                end = next_close.end()
                search_from = next_close.end()
                if depth == 0:
                    break
        if end > start:
            results.append(xml[start:end])
            pos = end
        else:
            pos = after_open
    return results


def first_element_inner(xml: str, local_name: str) -> str | None:
    els = extract_elements(xml, local_name)
    if not els:
        return None
    return re.sub(r"^<[^>]+>", "", els[0]).rsplit("</", 1)[0]


def all_text_contents(xml: str, local_name: str) -> list[str]:
    return [t for t in (strip_tags(el) for el in extract_elements(xml, local_name)) if t]


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return (slug[:80] if slug else "unknown")


def parse_ccda_xml(xml: str, source_path: str = "inline.xml") -> dict[str, Any]:
    patient_role = (extract_elements(xml, "patientRole") or [""])[0]
    patient_block = (extract_elements(patient_role or xml, "patient") or [""])[0]
    givens = all_text_contents(patient_block, "given")
    families = all_text_contents(patient_block, "family")
    patient = " ".join([*givens, *families]).strip() or "Unknown patient"

    title_inner = first_element_inner(xml, "title")
    doc_title = strip_tags(title_inner) if title_inner else source_path

    gender_el = (extract_elements(patient_block, "administrativeGenderCode") or [""])[0]
    gender_m = re.search(r'\bdisplayName="([^"]+)"', gender_el, re.I) or re.search(
        r'\bcode="([^"]+)"', gender_el, re.I
    )
    gender_display = gender_m.group(1) if gender_m else None
    birth_m = re.search(
        r'\bvalue="([^"]+)"',
        (extract_elements(patient_block, "birthTime") or [""])[0],
        re.I,
    )
    birth = birth_m.group(1) if birth_m else None

    sections: list[dict[str, str]] = []
    seen: set[str] = set()
    for section in extract_elements(xml, "section"):
        t_inner = first_element_inner(section, "title")
        title = strip_tags(t_inner) if t_inner else "Untitled section"
        key = title.lower()
        if key in seen:
            continue
        narrative = first_element_inner(section, "text")
        body = strip_tags(narrative) if narrative else strip_tags(section)[:8000]
        if not body or len(body) < 8:
            continue
        seen.add(key)
        sections.append({"title": title, "slug": slugify(title), "body": body})

    overview_lines = [
        f"# {doc_title}",
        "",
        "## Patient demographics",
        f"- Name: {patient}",
    ]
    if gender_display:
        overview_lines.append(f"- Gender: {gender_display}")
    if birth:
        overview_lines.append(f"- Date of birth: {birth}")
    overview_lines += ["", "## Available clinical sections", *[f"- {s['title']}" for s in sections]]

    chunks: list[dict[str, Any]] = [
        {
            "kind": "xml-overview",
            "path": f"{source_path}#overview",
            "patient": patient,
            "content": "\n".join(overview_lines),
        }
    ]
    for s in sections:
        chunks.append(
            {
                "kind": "xml-section",
                "path": f"{source_path}#{s['slug']}",
                "patient": patient,
                "title": s["title"],
                "content": f"# {s['title']}\n\nPatient: {patient}\n\n{s['body']}",
            }
        )

    silver = {
        "patient": patient,
        "gender": gender_display,
        "birth_time": birth,
        "document_title": doc_title,
        "source_path": source_path,
        "section_titles": [s["title"] for s in sections],
        "section_count": len(sections),
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }
    return {"patient": patient, "chunks": chunks, "silver": silver, "sections": sections}
