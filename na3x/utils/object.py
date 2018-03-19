def obj_for_name(obj_name):
    """
    Instantiate class or function
    :param obj_name: class or function name
    :return: instance of class or function
    """
    parts = obj_name.split('.')
    module = ".".join(parts[:-1])
    m = __import__( module )
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m