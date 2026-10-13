import { createEffect, onCleanup, onMount } from 'solid-js';
import { Editor, Extension } from '@tiptap/core';
import StarterKit from '@tiptap/starter-kit';
import Placeholder from '@tiptap/extension-placeholder';
import {
  LoroSyncPlugin,
  LoroUndoPlugin,
  LoroEphemeralCursorPlugin,
  CursorEphemeralStore,
  undo,
  redo,
} from 'loro-prosemirror';
import { cursorColorFromName, type CrdtField } from '@spooky-sync/core';
import { keymap } from '@tiptap/pm/keymap';

interface CollaborativeEditorProps {
  field: CrdtField;
  placeholder?: string;
  class?: string;
  editable?: boolean;
  /** The document's plain text after every change (local or remote). */
  onUpdate?: (text: string) => void;
  username?: string;
}

export function CollaborativeEditor(props: CollaborativeEditorProps) {
  let containerRef: HTMLDivElement | undefined;
  let editor: Editor | undefined;
  let cursorPushTimer: ReturnType<typeof setTimeout> | undefined;
  let cursorUnsub: (() => void) | undefined;

  onMount(() => {
    if (!containerRef) return;

    const crdtField = props.field;
    const doc = crdtField.getDoc();

    // This session's cursor, shared with the others through the field's presence.
    const cursorColor = cursorColorFromName(props.username ?? 'Anonymous');
    const presence = new CursorEphemeralStore(doc.peerIdStr);
    presence.setLocal({
      user: { name: props.username ?? 'Anonymous', color: cursorColor },
    });

    editor = new Editor({
      element: containerRef,
      extensions: [
        // Loro's undo manager replaces ProseMirror's history.
        StarterKit.configure({ history: false }),
        Placeholder.configure({ placeholder: props.placeholder ?? 'Start typing...' }),
        Extension.create({
          name: 'loroCollaboration',
          addProseMirrorPlugins() {
            return [
              LoroSyncPlugin({ doc: doc as any }),
              LoroUndoPlugin({ doc }),
              keymap({ 'Mod-z': undo, 'Mod-y': redo, 'Mod-Shift-z': redo }),
            ];
          },
        }),
      ],
      editable: props.editable !== false,
      editorProps: { attributes: { class: 'outline-none' } },
      onUpdate: ({ editor: ed }) => props.onUpdate?.(ed.getText()),
    });

    // Install the cursor plugin via queueMicrotask so it lands in a
    // separate task from the editor's first render — that's all the
    // separation the previous setTimeout actually needed.
    const editorRef = editor;
    queueMicrotask(() => {
      try {
        const cursorPlugin = LoroEphemeralCursorPlugin(presence, {
          user: {
            name: props.username ?? 'Anonymous',
            color: cursorColor,
          },
        });
        const state = editorRef.view.state.reconfigure({
          plugins: [...editorRef.view.state.plugins, cursorPlugin],
        });
        editorRef.view.updateState(state);
      } catch (e) {
        console.warn('[CollaborativeEditor] Failed to add cursor plugin:', e);
      }

      // Publish local cursor changes as this session's presence.
      cursorUnsub = presence.subscribeBy((by) => {
        if (by !== 'local') return;
        if (cursorPushTimer) clearTimeout(cursorPushTimer);
        cursorPushTimer = setTimeout(() => {
          crdtField.pushCursorState(presence.encodeAll());
        }, 100);
      });

      // Import remote cursor state when it arrives
      crdtField.onCursorUpdate = (data: Uint8Array) => {
        try { presence.apply(data); } catch (e) {
          console.warn('[CollaborativeEditor] Failed to apply remote cursor:', e);
        }
      };
    });
  });

  createEffect(() => {
    if (editor) editor.setEditable(props.editable !== false);
  });

  onCleanup(() => {
    if (cursorUnsub) cursorUnsub();
    if (cursorPushTimer) clearTimeout(cursorPushTimer);
    editor?.destroy();
  });

  return <div ref={containerRef} class={props.class ?? ''} />;
}

