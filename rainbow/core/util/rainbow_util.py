# TODO: rename class name


from flatdict import FlatDict


def from_dict_to_list(dictionary: dict) -> list:
    """
    Return list of the given dictionary items
    """
    steps = []
    for k, v in dictionary.items():
        steps.append(k)
        steps.append(v)

    return steps


def reformat_dict_keys(params: dict, formatter: str) -> dict:
    """
    Use this to change all dictionary keys format according to given formatter
    """
    return {formatter.format(x): y for (x, y) in FlatDict(params).items()}
