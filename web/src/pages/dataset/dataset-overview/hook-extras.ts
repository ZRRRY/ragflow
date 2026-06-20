import { IDataset } from '@/interfaces/database/dataset';
import { getKbDetail } from '@/services/knowledge-service';
import { useQuery } from '@tanstack/react-query';
import { useParams, useSearchParams } from 'react-router';

const enum DatasetOverviewApiAction {
  FetchDatasetChunkCount = 'fetchDatasetChunkCount',
}

const DatasetOverviewKeys = {
  chunkCount: (datasetId?: string) =>
    [DatasetOverviewApiAction.FetchDatasetChunkCount, datasetId] as const,
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

export { useFetchDatasetChunkCount };
