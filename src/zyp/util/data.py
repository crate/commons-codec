def no_privates_no_nulls_no_empties(key, value) -> bool:
    """
    A filter for `attr.asdict`, to suppress private attributes.
    """
    is_private = key.name.startswith("_")
    is_null = value is None
    is_empty = value == []
    if is_private or is_null or is_empty:
        return False
    return True
