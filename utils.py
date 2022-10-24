from re import sub


def camel_case(s):
    """rewrite string in camelCase format"""
    s = sub(r"[_-]+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])
