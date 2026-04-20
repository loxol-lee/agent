from __future__ import annotations

from typing import Any

from core.contracts.model import Message


def _attachments_system_message(attachments: list[dict[str, Any]] | None) -> str | None:
    if not attachments:
        return None

    rows: list[str] = []
    for i, a in enumerate(attachments, start=1):
        if not isinstance(a, dict):
            continue
        name = a.get("file_name")
        mt = a.get("mime_type")
        sz = a.get("size_bytes")

        name_s = str(name).strip() if isinstance(name, str) and name.strip() else f"attachment_{i}"
        mt_s = str(mt).strip() if isinstance(mt, str) and mt.strip() else "application/octet-stream"
        sz_s = str(int(sz)) if isinstance(sz, int) and sz >= 0 else "unknown"

        rows.append(f"- 附件#{i}: name={name_s}, mime={mt_s}, size_bytes={sz_s}")

    if not rows:
        return None

    return (
        "本轮请求携带了附件元数据（仅元数据，不含原文）：\n"
        + "\n".join(rows)
        + "\n"
        + "当你的回答使用了附件信息，请在对应句末标注引用：[附件#序号]。"
    )


def build_messages(
    *,
    system_prompt: str,
    history: list[dict[str, Any]],
    user_text: str,
    memories: list[str] | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> list[Message]:
    base: list[Message] = [Message(role="system", content=system_prompt)]

    mems = [m.strip() for m in (memories or []) if isinstance(m, str) and m.strip()]
    if mems:
        base.append(
            Message(
                role="system",
                content="你之前记录的长期笔记（供参考，不要编造超出笔记的事实）：\n- " + "\n- ".join(mems[:50]),
            )
        )

    attachment_note = _attachments_system_message(attachments)
    if attachment_note:
        base.append(Message(role="system", content=attachment_note))

    for m in history:
        role = m.get("role")
        content = m.get("content")
        if isinstance(role, str) and isinstance(content, str):
            base.append(Message(role=role, content=content))

    base.append(Message(role="user", content=user_text))
    return base

