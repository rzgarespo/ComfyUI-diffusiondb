import os
import duckdb
from urllib.request import urlretrieve
import random

# Parquet details (save locally in the node folder)
PARQUET_URL = 'https://huggingface.co/datasets/poloclub/diffusiondb/resolve/main/metadata-large.parquet'
PARQUET_FILE = os.path.join(os.path.dirname(__file__), 'metadata-large.parquet')

class SDPromptNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mode": (["specific", "random"], {"default": "random"}),
                "number": ("INT", {"default": 1, "min": 1, "max": 14000000, "step": 1}),
                "filter": (["none", "sfw", "nsfw"], {"default": "none"}),
            },
            "hidden": {"unique_id": "UNIQUE_ID", "extra_pnginfo": "EXTRA_PNGINFO"},
        }

    RETURN_TYPES = ("STRING",)
    FUNCTION = "get_prompt"
    CATEGORY = "Prompts/SDPrompt"
    OUTPUT_NODE = True

    @classmethod
    def IS_CHANGED(cls, mode, number, filter, **kwargs):
        if mode == "random":
            return random.random()
        else:
            return f"{number}_{filter}"

    def get_prompt(self, mode, number, filter, unique_id=None, extra_pnginfo=None):
        if not os.path.exists(PARQUET_FILE):
            urlretrieve(PARQUET_URL, PARQUET_FILE)

        con = duckdb.connect()
        where_clause = ""
        if filter == "nsfw":
            where_clause = "WHERE (image_nsfw > 0.5 OR prompt_nsfw > 0.5)"
        elif filter == "sfw":
            where_clause = "WHERE (image_nsfw <= 0.5 AND prompt_nsfw <= 0.5)"

        count_query = f"SELECT COUNT(*) FROM '{PARQUET_FILE}' {where_clause}"
        total = con.execute(count_query).fetchone()[0]

        if total == 0:
            raise ValueError("No prompts match the filter.")

        if mode == "random":
            offset_val = random.randint(1, total) - 1
        else:
            if number > total:
                raise ValueError(f"Number exceeds matching prompts ({total})")
            offset_val = number - 1

        query = f"SELECT prompt FROM '{PARQUET_FILE}' {where_clause} LIMIT 1 OFFSET {offset_val}"
        prompt = con.execute(query).fetchone()[0]

        con.close()

        
        if unique_id is not None and extra_pnginfo is not None:
            if isinstance(extra_pnginfo, list) and isinstance(extra_pnginfo[0], dict) and "workflow" in extra_pnginfo[0]:
                workflow = extra_pnginfo[0]["workflow"]
                node = next((x for x in workflow["nodes"] if str(x["id"]) == unique_id), None)
                if node:
                    node["widgets_values"] = [prompt]

        return {"ui": {"text": [prompt]}, "result": (prompt,)}


NODE_CLASS_MAPPINGS = {
    "SDPromptNode": SDPromptNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SDPromptNode": "🎨 SDPrompt"
}
