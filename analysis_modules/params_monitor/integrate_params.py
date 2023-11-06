
from analysis_modules.params_monitor.import_params import ImportParams
from analysis_modules.params_monitor.output_params import OutputParams
from analysis_modules.params_monitor.basic_process_params import BasicProcessParams
from analysis_modules.params_monitor.resources_operation import ResourcesOperation
from analysis_modules.params_monitor.sys_log import SysLog
import analysis_modules.default_properties as prop

class IntegrateParams:
    def __init__(self):
        pass

    @staticmethod
    def get_params_from_settings(params_set: str = prop.DEFAULT_PARAMS_SET) -> tuple[ImportParams, OutputParams, BasicProcessParams]:
        ro = ResourcesOperation()
        import_params = ImportParams()
        output_params = OutputParams()
        basic_process_params = BasicProcessParams()
        ro.remove_resources_file(params_set)
        import_params.store_import_params(params_set)
        output_params.store_output_params(params_set)
        basic_process_params.store_basic_process_params(params_set)
        SysLog.show_log(f"[PARAMS] parameters initialization from settings, params set file '{params_set}.json' is created or overwritten.")
        return import_params, output_params, basic_process_params

    @staticmethod
    def get_params_from_resources(params_set: str=prop.DEFAULT_PARAMS_SET) -> tuple[ImportParams, OutputParams, BasicProcessParams]:
        import_params = ImportParams()
        output_params = OutputParams()
        basic_process_params = BasicProcessParams()
        import_params.load_import_params(params_set)
        output_params.load_output_params(params_set)
        basic_process_params.load_basic_process_params(params_set)
        SysLog.show_log(
            f"[PARAMS] parameters initialization from existing params file '{params_set}.json'.")
        return import_params, output_params, basic_process_params


if __name__ == "__main__":
    import_params, output_params, basic_process_params = IntegrateParams.get_params_from_settings()
    import_params, output_params, basic_process_params = IntegrateParams.get_params_from_resources()