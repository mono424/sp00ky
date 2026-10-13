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
  onUpdate?: (text: string) => void;
  singleLine?: boolean;
  username?: string;
}

export function CollaborativeEditor(props: CollaborativeEditorProps) {
  let containerRef: HTMLDivElement | undefined;
  let editor: Editor | undefined;
  let userHasInteracted = false;
  let lastEmittedText: string | undefined;
  let suppressOnUpdate = true;
  let presence: CursorEphemeralStore | undefined;
  let cursorPushTimer: ReturnType<typeof setTimeout> | undefined;
  let cursorUnsub: (() => void) | undefined;

  onMount(() => {
    if (!containerRef) return;

    const crdtField = props.field;
    const doc = crdtField.getDoc();

    // Create cursor presence store
    const cursorColor = cursorColorFromName(props.username ?? 'Anonymous');
    presence = new CursorEphemeralStore(doc.peerIdStr);
    presence.setLocal({
      user: { name: props.username ?? 'Anonymous', color: cursorColor },
    });

    const presenceRef = presence;

    editor = new Editor({
      element: containerRef,
      extensions: [
        StarterKit.configure({
          history: false,
          ...(props.singleLine ? {
            heading: false, bulletList: false, orderedList: false,
            blockquote: false, codeBlock: false, horizontalRule: false, hardBreak: false,
          } : {}),
        }),
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
      editorProps: {
        attributes: { class: 'outline-none' },
        ...(props.singleLine ? {
          handleKeyDown: (_view: any, event: KeyboardEvent) => {
            if (event.key === 'Enter') { event.preventDefault(); return true; }
            return false;
          },
        } : {}),
      },
      onFocus: () => { userHasInteracted = true; },
      onUpdate: ({ editor: ed }) => {
        if (suppressOnUpdate || !userHasInteracted) return;
        const text = ed.getText();
        if (text === lastEmittedText) return;
        lastEmittedText = text;
        props.onUpdate?.(text);
      },
    });

    lastEmittedText = editor.getText();

    // The editor is mounted and seeded — the user couldn't have typed
    // anything yet, so it's safe to start emitting onUpdate now. (The old
    // 200 ms gate was a defensive guard that delayed remote-cursor
    // delivery in collab sessions for no benefit.)
    suppressOnUpdate = false;

    // Install the cursor plugin via queueMicrotask so it lands in a
    // separate task from the editor's first render — that's all the
    // separation the previous setTimeout actually needed.
    const editorRef = editor;
    queueMicrotask(() => {
      try {
        const cursorPlugin = LoroEphemeralCursorPlugin(presenceRef, {
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
      cursorUnsub = presenceRef.subscribeBy((by) => {
        if (by !== 'local') return;
        if (cursorPushTimer) clearTimeout(cursorPushTimer);
        cursorPushTimer = setTimeout(() => {
          crdtField.pushCursorState(presenceRef.encodeAll());
        }, 100);
      });

      // Import remote cursor state when it arrives
      crdtField.onCursorUpdate = (data: Uint8Array) => {
        try { presenceRef.apply(data); } catch (e) {
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

