from string import Template


class CfgUtils:
    @staticmethod
    def substitute_params(cfg, params):
        return  Template(cfg).safe_substitute(params)
