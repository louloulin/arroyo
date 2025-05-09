import Editor from '@monaco-editor/react';
import React, { Dispatch, useEffect } from 'react';
import { Flex } from '@chakra-ui/react';
import { setupMonacoLanguages } from '../../lib/monaco-setup';

export function CodeEditor({
  code,
  setCode,
  readOnly,
  language,
}: {
  code: string;
  setCode?: Dispatch<string>;
  readOnly?: boolean;
  language?: string;
}) {
  // Setup Monaco languages when component mounts
  useEffect(() => {
    setupMonacoLanguages();
  }, []);

  const onChange = (value: string | undefined) => {
    if (setCode != null) {
      setCode(value || '');
    }
  };

  return (
    <Flex py={5} pr={5} flex={1}>
      <Editor
        defaultLanguage={language || 'sql'}
        onChange={onChange}
        theme="vs-dark"
        options={{ minimap: { enabled: false }, wordWrap: 'on', readOnly: readOnly || false }}
        value={code}
      />
    </Flex>
  );
}
