import { TablePackModal, type BulkTable } from "./TablePackModal";
import { SyncDdlDialog, type SyncGroup, type SyncSelection } from "./SyncDdlDialog";
import { PACK_DEFINITIONS, type PackAddMode, type TablePackAddMode } from "./packModel";
import type {
  AddPlanItemsResp,
  MigrationPlanCdcGroup,
} from "./api";

interface BaseProps {
  schemaMigrationId: string;
  onClose: () => void;
}

interface DdlPackProps extends BaseProps {
  mode: Extract<PackAddMode, "ddl">;
  ddlGroups: SyncGroup[];
  onDdlSubmit: (selection: SyncSelection[]) => Promise<void>;
}

interface TablePackProps extends BaseProps {
  mode: TablePackAddMode;
  tables: BulkTable[];
  cdcGroup?: MigrationPlanCdcGroup | null;
  cdcGroupLoading?: boolean;
  cdcGroupError?: string | null;
  onReloadCdcGroup?: () => void | Promise<void>;
  onDone: (packQueueId: number, count: number, response: AddPlanItemsResp) => void | Promise<void>;
}

type Props = DdlPackProps | TablePackProps;

export function AddToPackModal(props: Props) {
  if (props.mode === "ddl") {
    return (
      <SyncDdlDialog
        groups={props.ddlGroups}
        title={`Добавить в ${PACK_DEFINITIONS.ddl.title}`}
        submitLabel="В пачку"
        onClose={props.onClose}
        onSubmit={props.onDdlSubmit}
      />
    );
  }

  return (
    <TablePackModal
      schemaMigrationId={props.schemaMigrationId}
      tables={props.tables}
      initialMode={props.mode}
      modeLocked
      cdcGroup={props.cdcGroup}
      cdcGroupLoading={props.cdcGroupLoading ?? false}
      cdcGroupError={props.cdcGroupError ?? null}
      onClose={props.onClose}
      onReloadCdcGroup={props.onReloadCdcGroup}
      onDone={props.onDone}
    />
  );
}
