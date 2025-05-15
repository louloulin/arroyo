import { Stack } from '@chakra-ui/react';
import { JsonForm } from './JsonForm';
import { Connector, useConnectionProfileAutocomplete } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';
import { PushConnectionForm } from './push/PushConnectionForm';

export const ConfigureConnection = ({
  connector,
  onSubmit,
  state,
  setState,
}: {
  connector: Connector;
  onSubmit: () => void;
  state: CreateConnectionState;
  setState: (s: CreateConnectionState) => void;
}) => {
  const { autocompleteData, autocompleteError } = state.connectionProfileId
    ? useConnectionProfileAutocomplete(state.connectionProfileId)
    : { autocompleteData: undefined, autocompleteError: null };

  // 如果是 Push Connector，使用自定义表单
  if (connector.id === 'push') {
    return (
      <PushConnectionForm
        connector={connector}
        state={state}
        setState={setState}
        onSubmit={onSubmit}
      />
    );
  }

  // 其他连接器使用通用 JsonForm
  return (
    <Stack spacing={8}>
      <Stack spacing="4" maxW={800}>
        <JsonForm
          schema={JSON.parse(connector.tableConfig)}
          initial={state.table || {}}
          onSubmit={async table => {
            setState({ ...state, table: table });
            onSubmit();
          }}
          error={null}
          button={'Next'}
          autocompleteData={autocompleteData}
          autocompleteError={autocompleteError?.error}
        />
      </Stack>
    </Stack>
  );
};
