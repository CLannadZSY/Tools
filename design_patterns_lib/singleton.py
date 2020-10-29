class Singleton(type):
    """
    Singleton Metaclass.
    """
    __instance = None

    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args, **kwargs)

        return cls.__instance
