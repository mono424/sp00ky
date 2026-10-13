// Recipe: crdt-text-field
// A collaborative textarea for `{{table}}.{{field}}`, declared in the schema as
//
//   -- @crdt text
//   DEFINE FIELD {{field}} ON TABLE {{table}} TYPE string;
//
// The editor binds to the field's Loro document. Each client sends only its own
// edits (rows of the generated `{{table}}__crdt_log` table, through the outbox)
// and merges everyone else's as they sync; the server folds them into the
// canonical document. The `{{field}}` column itself is the server-written plain
// text: render it for previews, never write it with `db.create` / `db.update`.

import { createEffect, createSignal, onCleanup, Show } from 'solid-js';
import { useCrdtField, useDb, useQuery } from '@spooky-sync/client-solid';
import { schema } from '../schema.gen';

export function {{TablePascal}}{{FieldPascal}}Editor(props: { id: string }) {
  // 3 arguments: table, a reactive record id, field. Reopens when the id changes.
  const field = useCrdtField('{{table}}', () => props.id, '{{field}}');
  const [text, setText] = createSignal('');

  // Follow the document: local and remote edits alike re-render the textarea.
  createEffect(() => {
    const doc = field()?.getDoc();
    if (!doc) return;
    const loroText = doc.getText('{{field}}');
    setText(loroText.toString());
    onCleanup(doc.subscribe(() => setText(loroText.toString())));
  });

  const handleInput = (e: InputEvent & { currentTarget: HTMLTextAreaElement }) => {
    const doc = field()?.getDoc();
    if (!doc) return;
    // Diff into the document rather than replacing it, so concurrent edits merge.
    doc.getText('{{field}}').update(e.currentTarget.value);
    doc.commit();
  };

  return (
    <Show when={field()} fallback={<p>Loading...</p>}>
      <textarea value={text()} onInput={handleInput} />
    </Show>
  );
}

// Previews read the column like any other. It is the document's text as of the
// server's last fold, so it trails live edits (a few seconds by default).
export function {{TablePascal}}{{FieldPascal}}Preview(props: { id: string }) {
  const db = useDb<typeof schema>();
  const result = useQuery(() =>
    db.query('{{table}}').where({ id: props.id } as any).build()
  );
  return <p>{result.data()?.[0]?.{{field}}}</p>;
}
