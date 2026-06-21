# === CUSTOM BEGIN [siliconflow-timeout] ===
# 原因：SiliconFlow Embedding API 在 GraphRAG 批量 embedding 时 30s 经常超时，
#      通过 monkey patch 把超时改为可配置，避免直接修改官方 embedding_model.py。
# 日期：2026-06-21
# 关联：rag/graphrag/config.py
# === CUSTOM END [siliconflow-timeout] ===

import logging
import os

import requests

from rag.llm.embedding_model import SILICONFLOWEmbed

logger = logging.getLogger(__name__)

# 保留原始 _call，避免影响继承自 SILICONFLOWEmbed 的其他 provider（如 NovitaAI、GiteeAI）。
_original_siliconflow_call = SILICONFLOWEmbed._call


def _call_with_timeout(self, batch):
    """仅对 SILICONFLOWEmbed 实例使用可配置超时，其他子类保持官方默认行为。"""
    if type(self) is not SILICONFLOWEmbed:
        return _original_siliconflow_call(self, batch)

    payload = {
        "model": self.model_name,
        "input": self._clean_batch(batch),
        "encoding_format": "float",
    }
    timeout = int(os.environ.get("SILICONFLOW_TIMEOUT", "120"))
    response = requests.post(
        self.base_url,
        json=payload,
        headers=self.headers,
        timeout=timeout,
    )
    return self._openai_http_embeddings(response)


def install():
    """将 SILICONFLOWEmbed._call 替换为支持环境变量配置超时的版本。"""
    if getattr(SILICONFLOWEmbed._call, "_siliconflow_timeout_patched", False):
        return
    SILICONFLOWEmbed._call = _call_with_timeout
    setattr(SILICONFLOWEmbed._call, "_siliconflow_timeout_patched", True)
    logger.info("SILICONFLOWEmbed timeout patch installed (env SILICONFLOW_TIMEOUT, default 120s).")
