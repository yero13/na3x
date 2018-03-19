from string import Template


class CfgUtils:
    @staticmethod
    def substitute_params(cfg, params):
        """
        Replaces $ parameters in configuration
        :param cfg: configuration (text)
        :param params: parameters to substitute
        :return: configuration with substituted parameters
        """
        return  Template(cfg).safe_substitute(params)
