import { useHandleFilterSubmit } from '@/components/list-filter-bar/use-handle-filter-submit';
import {
  useGetPaginationWithRouter,
  useHandleSearchChange,
} from '@/hooks/logic-hooks';
import { IDataset } from '@/interfaces/database/dataset';
import {
  getKbDetail,
  getKnowledgeBasicInfo,
  listDataPipelineLogDocument,
} from '@/services/knowledge-service';
import { useQuery } from '@tanstack/react-query';
import { useCallback, useState } from 'react';
import { useParams, useSearchParams } from 'react-router';
import { LogTabs } from './dataset-common';
import { IFileLogList, IOverviewTotal } from './interface';

const enum DatasetOverviewApiAction {
  FetchDatasetChunkCount = 'fetchDatasetChunkCount',
}

const DatasetOverviewKeys = {
  chunkCount: (datasetId?: string) =>
    [DatasetOverviewApiAction.FetchDatasetChunkCount, datasetId] as const,
};

const useFetchOverviewTotal = () => {
  const [searchParams] = useSearchParams();
  const { id } = useParams();
  const knowledgeBaseId = searchParams.get('id') || id;
  const { data } = useQuery<IOverviewTotal>({
    queryKey: ['overviewTotal'],
    queryFn: async () => {
      const { data: res = {} } = await getKnowledgeBasicInfo(
        knowledgeBaseId || '',
      );
      return res.data || [];
    },
  });
  return { data };
};

// Mirrors the file-count strategy used in this page: fetched once on mount
// with no active polling. The chunk_count comes from the KB detail endpoint
// and is refetched on stale window expiry / manual invalidation. There is
// currently NO explicit link from useFetchDocumentList's query key to this
// query — earlier drafts claimed otherwise but the wiring was never added.
// If on-demand refresh after doc changes is required, wire it explicitly
// via queryClient.invalidateQueries(DatasetOverviewKeys.chunkCount(id)) in
// the document-mutation hooks.
const useFetchDatasetChunkCount = () => {
  const [searchParams] = useSearchParams();
  const { id } = useParams();
  const knowledgeBaseId = searchParams.get('id') || id;
  const { data } = useQuery<number>({
    queryKey: DatasetOverviewKeys.chunkCount(knowledgeBaseId),
    enabled: !!knowledgeBaseId,
    queryFn: async () => {
      const { data: res = {} } = await getKbDetail(knowledgeBaseId || '');
      const detail: IDataset | undefined = res.data;
      return detail?.chunk_count ?? 0;
    },
  });
  return { data };
};

const useFetchFileLogList = () => {
  const [searchParams] = useSearchParams();
  const { searchString, handleInputChange } = useHandleSearchChange();
  const { pagination, setPagination } = useGetPaginationWithRouter();
  const { filterValue, setFilterValue, handleFilterSubmit } =
    useHandleFilterSubmit();
  const { id } = useParams();
  const [active, setActive] = useState<(typeof LogTabs)[keyof typeof LogTabs]>(
    LogTabs.FILE_LOGS,
  );
  const knowledgeBaseId = searchParams.get('id') || id;
  const logType = active === LogTabs.DATASET_LOGS ? 'dataset' : 'file';
  const { data } = useQuery<IFileLogList>({
    queryKey: [
      'fileLogList',
      knowledgeBaseId,
      pagination,
      searchString,
      active,
      filterValue,
    ],
    placeholderData: (previousData) => {
      if (previousData === undefined) {
        return { logs: [], total: 0 };
      }
      return previousData;
    },
    enabled: true,
    queryFn: async () => {
      const { data: res = {} } = await listDataPipelineLogDocument(
        knowledgeBaseId || '',
        {
          page: pagination.current,
          page_size: pagination.pageSize,
          keywords: searchString,
          log_type: logType,
          ...filterValue,
        },
      );
      return res.data || [];
    },
  });
  const onInputChange: React.ChangeEventHandler<HTMLInputElement> = useCallback(
    (e) => {
      setPagination({ page: 1 });
      handleInputChange(e);
    },
    [handleInputChange, setPagination],
  );
  return {
    data,
    searchString,
    handleInputChange: onInputChange,
    pagination: { ...pagination, total: data?.total },
    setPagination,
    active,
    setActive,
    filterValue,
    setFilterValue,
    handleFilterSubmit,
  };
};

export {
  useFetchDatasetChunkCount,
  useFetchFileLogList,
  useFetchOverviewTotal,
};
