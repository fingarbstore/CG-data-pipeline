import json


def strip_gid(gid):
    if gid is None:
        return None
    return gid.split("/")[-1]


def extract_stamped_tier(tags):
    for tag in (tags or []):
        if tag.startswith("StampedVIPTier:"):
            return tag.split(":", 1)[1].strip()
    return None


def parse_product_tags(tags):
    mapping = {
        "Colour":       "tag_colour",
        "Department":   "tag_department",
        "Gender":       "tag_gender",
        "Season":       "tag_season",
        "Sub-category": "tag_category",
        "Price":        "tag_price_status",
        "RetailProID":  "retail_pro_id",
    }
    result = {}
    for tag in (tags or []):
        if ":" in tag:
            key, _, value = tag.partition(":")
            key = key.strip()
            value = value.strip()
            if key in mapping and value:
                result[mapping[key]] = value
    return result
