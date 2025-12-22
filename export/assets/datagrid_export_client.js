// Datagrid export helper for go-admin DataGrid -> go-export DatagridRequest.
// Intended to be used with exportcrud.DatagridRequest payloads.
//
// Example usage:
// import { buildDatagridExportRequest } from './datagrid_export_client.js';
//
// const payload = buildDatagridExportRequest(grid, {
//   definition: 'users',
//   format: 'xlsx',
//   delivery: 'auto'
// });
//
// fetch('/admin/exports', {
//   method: 'POST',
//   headers: { 'Content-Type': 'application/json' },
//   body: JSON.stringify(payload)
// });
//
// Shipping:
// - go-export exposes RuntimeAssetsFS() so Go apps can serve this file without an npm build.
// - Or copy this file into your app's static assets and serve it from a stable URL,
//   e.g. /assets/datagrid_export_client.js.
// - Load it with a module script tag:
//   <script type="module" src="/assets/datagrid_export_client.js"></script>

export function buildDatagridExportRequest(grid, options = {}) {
  if (!grid) {
    throw new Error('datagrid is required');
  }
  const definition = String(options.definition || '').trim();
  if (!definition) {
    throw new Error('definition is required');
  }

  const format = normalizeFormat(options.format || 'csv');
  const selection = buildSelection(grid, options);
  const columns = buildVisibleColumns(grid, options);
  const query = buildQuery(grid, options);

  return {
    definition,
    format,
    query,
    selection,
    columns,
    delivery: options.delivery || 'auto',
    estimated_rows: options.estimatedRows || 0,
    estimated_bytes: options.estimatedBytes || 0
  };
}

export function buildDatagridQuery(grid) {
  return buildQuery(grid, {});
}

function normalizeFormat(format) {
  const value = String(format || '').trim().toLowerCase();
  if (value === 'excel' || value === 'xls') return 'xlsx';
  return value || 'csv';
}

function buildSelection(grid, options) {
  if (options.selection && options.selection.mode) {
    return options.selection;
  }
  const selected = Array.from(grid.state?.selectedRows || []);
  const mode = options.selectionMode || (selected.length > 0 ? 'ids' : 'all');
  return {
    mode,
    ids: mode === 'ids' ? selected : []
  };
}

function buildVisibleColumns(grid, options) {
  if (Array.isArray(options.columns) && options.columns.length > 0) {
    return options.columns.slice();
  }
  const hidden = grid.state?.hiddenColumns ? new Set(grid.state.hiddenColumns) : new Set();
  const order = Array.isArray(grid.state?.columnOrder) && grid.state.columnOrder.length > 0
    ? grid.state.columnOrder
    : grid.config?.columns?.map((col) => col.field) || [];
  return order.filter((field) => !hidden.has(field));
}

function buildQuery(grid, options) {
  const state = grid.state || {};
  const filters = Array.isArray(state.filters)
    ? state.filters
        .filter((filter) => filter && filter.column)
        .map((filter) => ({
          field: filter.column,
          op: filter.operator || 'eq',
          value: filter.value
        }))
    : [];
  const sort = Array.isArray(state.sort)
    ? state.sort
        .filter((spec) => spec && spec.field)
        .map((spec) => ({
          field: spec.field,
          desc: spec.direction === 'desc'
        }))
    : [];
  const perPage = Number(state.perPage || 0);
  const currentPage = Number(state.currentPage || 1);
  const offset = perPage > 0 ? (currentPage - 1) * perPage : 0;

  return {
    filters,
    search: state.search || '',
    sort,
    limit: perPage,
    offset,
    ...options.query
  };
}
