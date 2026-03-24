class CompilationError(Exception):
    """
    SQL编译过程中出现的错误。

    当整个编译流程出现无法继续的错误时抛出。
    """
    pass


class SQLParseError(Exception):
    """
    SQL解析过程中出现的语法或语义错误。

    当SQL无法被正确解析时抛出，通常表示SQL语法错误。
    """
    pass


class GraphBuildError(Exception):
    """
    图谱构建过程中出现的错误。

    当无法正确构建知识图谱时抛出，如图谱结构不完整等。
    """
    pass


class ConfigurationError(Exception):
    """
    配置相关的错误。

    当配置文件格式错误或配置参数无效时抛出。
    """
    pass

class ConfigLoadError(ConfigurationError):
    """
        用户提供的配置不存在时抛出错误
    """
    pass