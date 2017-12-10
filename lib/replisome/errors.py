class ReplisomeError(Exception):
    """Control exceptions: to exit politely"""
    pass


class ConfigError(ReplisomeError):
    """Error in some configuration entry"""
    pass
