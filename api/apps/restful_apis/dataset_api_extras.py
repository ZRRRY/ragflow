#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import logging

from common.constants import RetCode
from api.apps import login_required
from api.utils.api_utils import get_error_data_result, get_result, add_tenant_id_to_kwargs
from api.apps.services import dataset_api_service


def delete_knowledge_graph(tenant_id, dataset_id):
    """Delete the knowledge graph of a dataset.

    DELETE /api/v1/datasets/<dataset_id>/graph
    Success: {"code": 0, "data": True}
    Errors:
      * ``AUTHENTICATION_ERROR`` — caller has no access to the dataset
        (RAGFlow project convention: ``AUTHENTICATION_ERROR`` is used here as
        the catch-all "no authorization" code, even though HTTP-wise the
        situation is closer to 403 Forbidden. This matches the existing
        pattern in ``document_api.delete_document`` and
        ``dify_retrieval_api.delete_dataset``.)
      * ``DATA_ERROR`` — internal server error.
    """
    try:
        success, result = dataset_api_service.delete_knowledge_graph(dataset_id, tenant_id)
        if success:
            return get_result(data=result)
        else:
            return get_result(data=False, message=result, code=RetCode.AUTHENTICATION_ERROR)
    except Exception as e:
        logging.exception(e)
        return get_error_data_result(message="Internal server error")


# Register the route when this module is loaded by api.apps.register_page, which
# injects ``manager`` into the module namespace.  Keeping the handler function
# free of direct ``manager`` references allows other modules (and unit tests) to
# import ``delete_knowledge_graph`` directly without a Quart blueprint context.
try:
    _manager = manager  # noqa: F821
except NameError:
    _manager = None

if _manager is not None:
    _manager.route("/datasets/<dataset_id>/graph", methods=["DELETE"])(
        login_required(add_tenant_id_to_kwargs(delete_knowledge_graph))
    )
