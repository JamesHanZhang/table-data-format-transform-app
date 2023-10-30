
from analysis_modules.params_monitor.import_params import ImportParams
from analysis_modules.params_monitor.output_params import OutputParams
from analysis_modules.params_monitor.resources_operation import ResourcesOperation
from analysis_modules.params_monitor.sys_log import SysLog
import analysis_modules.default_properties as prop

class IntegrateParams:
    def __init__(self):
        pass

    @staticmethod
    @SysLog().direct_show_log("[PARAMS] parameters initialization from settings.")
    def get_params_from_settings(params_set: str = prop.DEFAULT_PARAMS_SET):
        ro = ResourcesOperation()
        import_params = ImportParams()
        output_params = OutputParams()
        ro.remove_resources_file(params_set)
        import_params.store_import_params(params_set)
        output_params.store_output_params(params_set)
        return import_params, output_params

    @staticmethod
    @SysLog().direct_show_log("[PARAMS] parameters initialization from settings.")
    def get_params_from_resources(params_set: str=prop.DEFAULT_PARAMS_SET):
        import_params = ImportParams()
        output_params = OutputParams()
        import_params.load_import_params(params_set)
        output_params.load_output_params(params_set)
        return import_params, output_params


if __name__ == "__main__":
    import_params, output_params = IntegrateParams.get_params_from_settings()
    import_params, output_params = IntegrateParams.get_params_from_resources()