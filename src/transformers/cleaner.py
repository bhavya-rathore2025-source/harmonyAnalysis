"""Generic URL normalizer, keyword categorizer, and schema standardizer.
Adaptable to any website without hardcoding domain names.
"""
from urllib.parse import urlparse
from typing import List, Dict, Any

def normalize_url(url: str) -> str:
    """Removes query parameters, hash fragments, protocols, and trailing slashes.
    Example: 'https://harmonybreath.com/blog/techniques/?utm_source=fb/' -> 'harmonybreath.com/blog/techniques'
    """
    if not url:
        return ""
    
    parsed = urlparse(url)
    domain_and_path = f"{parsed.netloc}{parsed.path}".rstrip("/")
    # If URL had no protocol scheme originally
    if not domain_and_path and url:
        domain_and_path = url.split("?")[0].split("#")[0].rstrip("/")
    return domain_and_path.lower()

def extract_url_category(url: str) -> str:
    """Dynamically extracts the first directory segment as the category.
    Example: 'harmonybreath.com/blog/breathing' -> 'blog'
    Example: 'harmonybreath.com/' -> 'home'
    Works dynamically for ANY website structure!
    """
    if not url:
        return "unknown"
    
    parsed = urlparse(url if "://" in url else f"http://{url}")
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    
    if not path_parts:
        return "home"
    return path_parts[0].lower()

def classify_keyword(query: str, brand_terms: List[str]) -> Dict[str, Any]:
    """Classifies a search query dynamically:
    1. brand_type: 'Brand' vs 'Non-Brand' based on brand_terms list.
    2. length_type: 'Short-Tail' (1-2 words) vs 'Long-Tail' (3+ words).
    """
    clean_query = query.strip().lower() if query else ""
    words = clean_query.split()
    word_count = len(words)
    
    is_brand = any(brand in clean_query for brand in brand_terms) if clean_query else False
    
    return {
        "brand_type": "Brand" if is_brand else "Non-Brand",
        "length_type": "Short-Tail" if word_count <= 2 else "Long-Tail",
        "word_count": word_count
    }
