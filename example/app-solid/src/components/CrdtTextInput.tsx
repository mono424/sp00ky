import { createEffect, createSignal, onCleanup } from 'solid-js';
import type { CrdtField } from '@spooky-sync/core';

type Cursor = ReturnType<import('loro-crdt').LoroText['getCursor']>;

/**
 * A one-line input bound to a `@crdt text` field. Typing diffs into the
 * field's `LoroText`, so edits made at the same time on other devices merge
 * instead of overwriting each other; their edits re-render the input in
 * place, keeping the caret where it was in the text.
 *
 * Until the field is open it shows `fallback` (the column's text) read-only.
 * `readOnly` keeps it read-only for good while still following live edits.
 */
export function CrdtTextInput(props: {
  field: CrdtField | null;
  /** The field name: a `@crdt text` document keeps its text under it. */
  name: string;
  fallback: string;
  placeholder?: string;
  maxLength?: number;
  class?: string;
  id?: string;
  readOnly?: boolean;
}) {
  const [text, setText] = createSignal(props.fallback);
  let input: HTMLInputElement | undefined;
  // The caret as a Loro cursor: it follows the text through remote edits.
  let caret: Cursor;

  const loroText = () => props.field?.getDoc().getText(props.name);

  const rememberCaret = () => {
    const t = loroText();
    if (!t || !input || document.activeElement !== input) return;
    caret = t.getCursor(input.selectionStart ?? t.length);
  };

  createEffect(() => {
    const doc = props.field?.getDoc();
    const t = loroText();
    if (!doc || !t) return;
    const render = () => {
      const next = t.toString();
      if (!input || input.value === next) return setText(next);
      const focused = document.activeElement === input;
      const pos = focused && caret ? doc.getCursorPos(caret)?.offset : undefined;
      setText(next);
      input.value = next;
      if (focused && typeof pos === 'number') input.setSelectionRange(pos, pos);
    };
    render();
    onCleanup(doc.subscribe(render));
  });

  const onInput = (e: InputEvent & { currentTarget: HTMLInputElement }) => {
    const doc = props.field?.getDoc();
    const t = loroText();
    if (!doc || !t || props.readOnly) return;
    t.update(e.currentTarget.value);
    doc.commit();
    rememberCaret();
  };

  return (
    <input
      ref={input}
      id={props.id}
      type="text"
      value={props.field ? text() : props.fallback}
      readOnly={!props.field || props.readOnly}
      onInput={onInput}
      onKeyUp={rememberCaret}
      onClick={rememberCaret}
      onSelect={rememberCaret}
      onFocus={rememberCaret}
      placeholder={props.placeholder}
      maxLength={props.maxLength}
      class={props.class}
    />
  );
}
