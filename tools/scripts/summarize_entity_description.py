"""
Standalone script for summarizing entity/relation descriptions, extracted from
rag/graphrag/general/extractor.py::_handle_entity_relation_summary.

Differences from the original logic:
  1. No truncation: the original caps the merged description at 512 tokens
     before summarizing; this script sends the full text.
  2. Trigger threshold lowered from 12 to 1: the original returns the raw
     joined description when it contains <= 12 fragments; this script calls
     the LLM whenever there is more than one fragment.

LLM access uses any OpenAI-compatible endpoint, configured via env vars:
  OPENAI_API_KEY    (required)
  OPENAI_BASE_URL   (optional, e.g. https://api.openai.com/v1)

Usage:
  python tools/scripts/summarize_entity_description.py \
      --model gpt-4o-mini \
      --entity-name "RAGFlow" \
      --description "desc part 1<SEP>desc part 2<SEP>desc part 3"

  # or read the description from a file:
  python tools/scripts/summarize_entity_description.py \
      --model gpt-4o-mini \
      --entity-name "RAGFlow" \
      --description-file description.txt
"""

import argparse
import os
import sys

from openai import OpenAI

GRAPH_FIELD_SEP = "<SEP>"

# Only summarize when there is more than one description fragment.
SUMMARIZE_THRESHOLD = 1

# Same prompt as rag/graphrag/general/graph_prompt.py::SUMMARIZE_DESCRIPTIONS_PROMPT
SUMMARIZE_DESCRIPTIONS_PROMPT = """
You are a helpful assistant responsible for generating a comprehensive summary of the data provided below.
Given one or two entities, and a list of descriptions, all related to the same entity or group of entities.
Please concatenate all of these into a single, comprehensive description. Make sure to include information collected from all the descriptions.
If the provided descriptions are contradictory, please resolve the contradictions and provide a single, coherent summary.
Make sure it is written in third person, and include the entity names so we the have full context.
Use {language} as output language.

#######
-Data-
Entities: {entity_name}
Description List: {description_list}
#######
"""


def summarize_description(client: OpenAI, model: str, entity_or_relation_name: str,
                          description: str, language: str) -> str:
    description_list = description.split(GRAPH_FIELD_SEP)
    if len(description_list) <= SUMMARIZE_THRESHOLD:
        return description

    use_prompt = SUMMARIZE_DESCRIPTIONS_PROMPT.format(
        entity_name=entity_or_relation_name,
        description_list=description_list,
        language=language,
    )
    print(f"Trigger summary: {entity_or_relation_name} "
          f"({len(description_list)} fragments)", file=sys.stderr)

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": use_prompt}],
    )
    return response.choices[0].message.content


def main():
    parser = argparse.ArgumentParser(
        description="Summarize merged entity/relation descriptions with an LLM.")
    parser.add_argument("--entity-name", required=True,
                        help="Entity name or relation string, e.g. 'Alice' or 'A -> B'")
    parser.add_argument("--description",
                        help="Merged description text, fragments separated by '<SEP>'")
    parser.add_argument("--description-file",
                        help="Read the description from this file instead of --description")
    parser.add_argument("--language", default="English",
                        help="Output language for the summary (default: English)")
    parser.add_argument("--model", default=os.environ.get("SUMMARIZE_MODEL", ""),
                        help="Chat model name (or set SUMMARIZE_MODEL env var)")
    args = parser.parse_args()

    if args.description_file:
        with open(args.description_file, encoding="utf-8") as f:
            description = f.read()
    elif args.description:
        description = args.description
    else:
        parser.error("one of --description or --description-file is required")

    if not args.model:
        parser.error("--model is required (or set SUMMARIZE_MODEL env var)")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        parser.error("OPENAI_API_KEY env var is required")

    client = OpenAI(api_key=api_key, base_url=os.environ.get("OPENAI_BASE_URL"))

    summary = summarize_description(client, args.model, args.entity_name,
                                    description, args.language)
    print(summary)


if __name__ == "__main__":
    main()
