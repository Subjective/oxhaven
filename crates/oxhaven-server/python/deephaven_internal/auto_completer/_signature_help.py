"""Signature help as Markdown: a signature of jedi and its docstring, taken apart by the
library `docstring_parser` (MIT licence), as the text the IDE shows beside a call.

Behavior: `py/server/deephaven_internal/auto_completer/_signature_help.py`
"""

from inspect import Parameter

from docstring_parser import parse

# what stands in a signature for "positional only before this" and "keyword only after"
_SEPARATORS = ("", "/", "*")


def _parameters(signature, docs):
    """What is known of each parameter of `signature`: `(name as shown, type, default,
    description)`, any of the last three `None`. The type is the annotation, or else the
    type the docstring gives."""
    documented = {
        param.arg_name.replace("*", ""): (param.type_name, param.description.strip())
        for param in docs.params
        if param.description
    }
    parameters = []
    for param in signature.params:
        text = param.to_string().strip()
        if param.kind == Parameter.VAR_POSITIONAL:
            shown = f"*{param.name}"
        elif param.kind == Parameter.VAR_KEYWORD:
            shown = f"**{param.name}"
        else:
            shown = param.name
        doc_type, description = documented.get(param.name, (None, None))
        if ":" in text:
            type_name = text.split(":")[1].split("=")[0].strip()
        else:
            type_name = doc_type
        default = text.split("=")[1] if "=" in text else None
        parameters.append((shown, type_name, default, description))
    return parameters


def _named(heading, entries):
    """A section of entries that have a type name and a description, such as what a
    function returns or raises."""
    if not entries:
        return ""
    text = f"#### **{heading}**\n\n"
    for entry in entries:
        if entry.type_name is not None:
            text += f"> **{entry.type_name}**  \n"
        if entry.description is not None:
            text += f"> {entry.description}"
        text += "\n\n"
    return text


def _description(docs, parameters):
    """The documentation of the whole signature."""
    text = ""
    if docs.description is not None:
        text = docs.description.strip().replace("\n", "  \n") + "\n\n"
    if parameters:
        text += "#### **Parameters**\n\n"
        for shown, type_name, default, description in parameters:
            if shown in _SEPARATORS:
                continue
            text += f"> **{shown}**"
            if type_name is not None:
                text += f": *{type_name}*"
            if default is not None:
                text += f" ⋅ (default: *{default}*)"
            text += "  \n"
            if description is not None:
                text += f"> {description}".replace("\n\n", "\n\n> ")
            text += "\n\n"
    text += _named("Returns", docs.many_returns)
    text += _named("Raises", docs.raises)
    if docs.examples:
        text += "#### **Examples**\n\n"
        for example in docs.examples:
            shown = example.description
            if shown is not None and shown.startswith(">>> "):
                shown = f"```\n{shown}\n```"
            text += (shown or "") + "\n\n"
    return text.strip()


def _parameter_docs(signature, parameters):
    """`[label, documentation]` of each parameter that is one."""
    docs = []
    for param, (shown, type_name, _, description) in zip(signature.params, parameters):
        label = param.to_string().strip()
        if label in _SEPARATORS:
            continue
        text = f"##### **{shown}**"
        if type_name is not None:
            text += f" : *{type_name}*"
        text += "\n\n"
        if description is not None:
            text += f"{description}\n\n"
        docs.append([label, text + "---"])
    return docs


def get_signature_help(signature) -> list:
    """`[label, documentation, [[parameter label, parameter documentation], ...], index of
    the active parameter or -1]` for a signature of jedi.

    A docstring that `docstring_parser` finds no sections in is shown as it is written,
    with its spaces and line ends kept in Markdown, and the parameters get no text.
    """
    label = signature.to_string()
    docstring = signature.docstring(raw=True)
    index = signature.index if signature.index is not None else -1
    docs = parse(docstring)
    if not docs.meta:
        as_written = docstring.replace(" ", "&nbsp;").replace("\n", "  \n")
        return [label, as_written, [[p.to_string().strip(), ""] for p in signature.params], index]
    parameters = _parameters(signature, docs)
    return [label, _description(docs, parameters), _parameter_docs(signature, parameters), index]
